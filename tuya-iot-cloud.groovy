/**
 * Tuya IoT Platform (Cloud) - Parent Driver (Hubitat)
 * Base: Jonathan Bradshaw + patches (ROPO + Towel-safe)
 */

import com.hubitat.app.ChildDeviceWrapper
import com.hubitat.app.DeviceWrapper
import com.hubitat.app.exception.UnknownDeviceTypeException
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.Field
import hubitat.scheduling.AsyncResponse
import hubitat.helper.HexUtils

import javax.crypto.Cipher
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import groovy.json.JsonSlurper
import groovy.json.JsonOutput

@Field static final String DRIVER_VERSION = "1.0.3"
@Field static final String DRIVER_DATE = "2026-01-18"


metadata {
    definition(name: 'Tuya IoT Platform (Cloud)', namespace: 'tuya', author: 'Ricardo Garcia + Jonathan Bradshaw + patches') {
        capability 'Initialize'
        capability 'Refresh'
        command 'removeDevices'

        attribute 'deviceCount', 'number'
        attribute 'state', 'enum', [
            'not configured','error','authenticating',
            'authenticated','connected','disconnected','ready'
        ]
    }

    preferences {
        section {
            input name: 'access_id', type: 'text', title: 'Tuya API Access/Client Id', required: true
            input name: 'access_key', type: 'password', title: 'Tuya API Access/Client Secret', required: true

            input name: 'appSchema', type: 'enum', title: 'Tuya Application',
                options: ['tuyaSmart':'Tuya Smart','smartlife':'Smart Life'],
                defaultValue: 'tuyaSmart', required: true

            input name: 'username', type: 'text', title: 'Tuya App Username', required: true
            input name: 'password', type: 'password', title: 'Tuya App Password', required: true

            input name: 'appCountry', type: 'enum', title: 'Country',
                options: tuyaCountries*.country, defaultValue: 'Brazil', required: true

            input name: 'logEnable', type: 'bool', title: 'Debug logging', defaultValue: true
            input name: 'txtEnable', type: 'bool', title: 'Description logging', defaultValue: true
            
            input name: "debugDumpDeviceId", type: "string",
      		title: "DEBUG – Dump JSON para Device ID",
     		description: "Exemplo: eba9c1a659f06b810dnvca",
      		required: false
            
            input name: "debugDumpPretty", type: "bool",
  			title: "DEBUG – Pretty print JSON", defaultValue: true

			input name: "debugDumpSaveState", type: "bool",
    		title: "DEBUG – Salvar último JSON em state", defaultValue: true

        }
    }
}

/* =========================================================
 *  CONSTANTS / MAPS
 * ========================================================= */

@Field static final JsonSlurper jsonParser = new JsonSlurper()
@Field static final Random random = new Random()
@Field static final Map<String, Map> jsonCache = new ConcurrentHashMap<>()

@Field static final Map<String, List<String>> tuyaFunctions = [
    power: ['switch','switch_1','switch_2','switch_3','switch_4','switch_5','switch_6'],
    light: ['switch_led','light'],
    temperature: ['temp_current','va_temperature'],
    temperatureSet: ['temp_set'],
    workState: ['work_state'],
    airQuality: ['air_quality'],
    filter: ['filter'],
    uv: ['uv'],
    fault: ['fault'],
    countdownSet: ['countdown_set'],
    countdownLeft: ['countdown_left']
].asImmutable()

/* =========================================================
 *  LOGGER
 * ========================================================= */

@Field private final Map LOG = [
    debug:{ s -> if (settings?.logEnable) log.debug s },
    info :{ s -> log.info s },
    warn :{ s -> log.warn s },
    error:{ s -> log.error s }
].asImmutable()

/* =========================================================
 *  LIFECYCLE
 * ========================================================= */

void installed() {
    LOG.info "Installed"
}

void uninstalled() {
    LOG.info "Uninstalled"
    try { interfaces.mqtt.disconnect() } catch (ignored) {}
}

void updated() {
    LOG.info "Updated v${DRIVER_VERSION} (${DRIVER_DATE})"
    if (settings?.logEnable) runIn(1800, 'logsOff')
    initialize()
}

void initialize() {
    LOG.info "Initializing"

    unschedule()
    try { interfaces.mqtt.disconnect() } catch (ignored) {}

    state.with {
        uuid = state?.uuid ?: UUID.randomUUID().toString()
        lang = 'en'
        tokenInfo = state?.tokenInfo ?: [access_token:'', refresh_token:'', uid:'', expire:0L]
        mqttInfo = state?.mqttInfo ?: [:]
        endPoint = state?.endPoint ?: null
        countryCode = state?.countryCode ?: null
    }

    sendEvent(name: 'deviceCount', value: 0)

    Map dc = tuyaCountries.find { it.country == settings?.appCountry }
    if (!dc) {
        sendEvent(name: 'state', value: 'error', descriptionText: 'Country not set / invalid')
        LOG.error "Country not set / invalid"
        return
    }

    state.endPoint = dc.endpoint
    state.countryCode = dc.countryCode

    tuyaAuthenticateAsync()
}

void refresh() {
    LOG.info "Refreshing"
    tuyaGetDevicesAsync()

    // Homes é opcional e está dando permission deny (1106) no seu projeto Tuya.
    // Deixe desabilitado para não “sujar” o estado/logs.
    // tuyaGetHomesAsync()
}

void removeDevices() {
    LOG.info "Removing child devices"
    childDevices.each { cd ->
        try { deleteChildDevice(cd.deviceNetworkId) } catch (ignored) {}
    }
}

/* =========================================================
 *  COMPONENT METHODS (called by child devices)
 * ========================================================= */

void componentSendTuyaCommand(DeviceWrapper dw, String code, Object value) {
    if (!dw?.getDataValue('id') || !code) return
    tuyaSendDeviceCommandsAsync(dw.getDataValue('id'), ['code': code, 'value': value])
}

void componentRefresh(DeviceWrapper dw) {
    String id = dw?.getDataValue('id')
    if (id) tuyaGetStateAsync(id)
}

// --- Compatibility overloads (Hubitat may pass DeviceWrapper) ---
void componentOn(com.hubitat.app.DeviceWrapper dw) { componentOnGeneric(dw) }
void componentOn(com.hubitat.app.ChildDeviceWrapper dw) { componentOnGeneric(dw) }

void componentOff(com.hubitat.app.DeviceWrapper dw) { componentOffGeneric(dw) }
void componentOff(com.hubitat.app.ChildDeviceWrapper dw) { componentOffGeneric(dw) }

// Prefer "switch" style DPs so ROPO doesn't break
private static String pickSwitchCode(Map<String, Map> functions) {
    List<String> preferred = [
        'switch', 'switch_1','switch_2','switch_3','switch_4','switch_5','switch_6',
        'switch_fan','switch_led','light'
    ]
    for (String c : preferred) {
        if (functions?.containsKey(c)) return c
    }
    return null
}

private void componentOnGeneric(def dw) {
    if (!dw?.getDataValue('id')) return
    Map<String, Map> functions = getFunctions(dw)
    String code = pickSwitchCode(functions)
    if (!code) code = getFunctionCode(functions, tuyaFunctions.light + tuyaFunctions.power) ?: 'switch'
    if (txtEnable) LOG.info "Turning ${dw} on (${code})"
    tuyaSendDeviceCommandsAsync(dw.getDataValue('id'), ['code': code, 'value': true])
}

private void componentOffGeneric(def dw) {
    if (!dw?.getDataValue('id')) return
    Map<String, Map> functions = getFunctions(dw)
    String code = pickSwitchCode(functions)
    if (!code) code = getFunctionCode(functions, tuyaFunctions.light + tuyaFunctions.power) ?: 'switch'
    if (txtEnable) LOG.info "Turning ${dw} off (${code})"
    tuyaSendDeviceCommandsAsync(dw.getDataValue('id'), ['code': code, 'value': false])
}

/* =========================================================
 *  CHILD CREATION / CATEGORY MAPPING
 * ========================================================= */

private static Map mapTuyaCategory(Map d) {
    Map st = d?.statusSet ?: [:]
    boolean looksPurifier = (st?.keySet()?.contains('air_quality') || st?.keySet()?.contains('filter') || st?.keySet()?.contains('uv'))
    boolean looksTowel = (d?.category == 'mjj' || (st?.keySet()?.contains('temp_set') && st?.keySet()?.contains('countdown_set')))

    if (looksTowel) {
        // your cleaned child driver
        return [namespace: 'tuya', driver: 'Tuya Component Towel Warmer']
    }
    if (looksPurifier) {
        // you must have this component driver installed (or adjust to your driver name)
        return [namespace: 'component', driver: 'Generic Component Air Purifier']
    }
    return [namespace: 'hubitat', driver: 'Generic Component Switch']
}

private boolean createChildDevices(Map d) {
    Map mapping = mapTuyaCategory(d)
    createChildDevice("${device.id}-${d.id}", mapping, d)
    return true
}

private ChildDeviceWrapper createChildDevice(String dni, Map mapping, Map d) {
    ChildDeviceWrapper cd = getChildDevice(dni)
    if (!cd) {
        LOG.info "Creating child ${d.name} as ${mapping.namespace}:${mapping.driver}"
        try {
            cd = addChildDevice(mapping.namespace ?: 'hubitat', mapping.driver, dni, [
                name : d.product_name ?: d.name ?: "Tuya Device",
                label: d.name ?: "Tuya Device"
            ])
        } catch (UnknownDeviceTypeException e) {
            LOG.error "Child driver not found: ${mapping.namespace}:${mapping.driver} (install it in Drivers Code)"
            return null
        } catch (e) {
            LOG.error "Failed creating child: ${e}"
            return null
        }
    }

    // Cache/store device metadata + functions/status sets
    String functionJson = JsonOutput.toJson(d.functions ?: [:])
    jsonCache.put(functionJson, d.functions ?: [:])

    cd?.with {
        updateDataValue('id', (d.id ?: ''))
        updateDataValue('local_key', (d.local_key ?: ''))
        updateDataValue('product_id', (d.product_id ?: ''))
        updateDataValue('category', (d.category ?: ''))
        updateDataValue('functions', functionJson)
        updateDataValue('statusSet', JsonOutput.toJson(d.statusSet ?: [:]))
        updateDataValue('online', (d.online as String))
    }
    return cd
}

/* =========================================================
 *  STATUS ROUTING (Tuya -> child.parse(events))
 * ========================================================= */

private void updateMultiDeviceStatus(Map d) {
    String devId = (d.id ?: d.devId)
    if (!devId) return

    String baseDni = "${device.id}-${devId}"
    ChildDeviceWrapper child = getChildDevice(baseDni)
    if (!child) return

    List<Map> status = d.status ?: []
    List<Map> evts = createEvents(child, status)
    if (evts) child.parse(evts)
}

private static Map<String, Object> toStatusByCode(List<Map> statusList) {
    return statusList?.collectEntries { [(it.code): it.value] } ?: [:]
}

private Map<String, Map> getFunctions(DeviceWrapper dw) {
    String raw = dw?.getDataValue('functions') ?: '{}'
    return jsonCache.computeIfAbsent(raw) { k -> (Map<String, Map>) jsonParser.parseText(k) }
}

private Map<String, Map> getStatusSet(DeviceWrapper dw) {
    String raw = dw?.getDataValue('statusSet') ?: '{}'
    return jsonCache.computeIfAbsent(raw) { k -> (Map<String, Map>) jsonParser.parseText(k) }
}

private static String getFunctionCode(Map functions, List codes) {
    return codes?.find { c -> functions?.containsKey(c) }
}

/**
 * IMPORTANT:
 * - We keep countdown_* as countdown (NOT global timer conversion), so ROPO and other devices don't get "timer contamination".
 * - Towel Warmer child can convert countdown -> timer if desired.
 */
private List<Map> createEvents(DeviceWrapper dw, List<Map> statusList) {
    Map<String, Object> statusByCode = toStatusByCode(statusList)
    Map<String, Map> deviceStatusSet = getStatusSet(dw) ?: getFunctions(dw) ?: [:]

    List<Map> out = []

    statusList.each { Map st ->
        String code = st.code
        def val = st.value

        // Switch handling
        if (code in (tuyaFunctions.power + tuyaFunctions.light)) {
            String sw = val ? 'on' : 'off'
            out << [name: 'switch', value: sw, descriptionText: "switch is ${sw}"]
            return
        }

        // ROPO: air quality
        if (code in tuyaFunctions.airQuality) {
            String raw = (val ?: '').toString()
            Map<String,String> map = ['great':'excellent','good':'good','ok':'fair','normal':'fair','poor':'poor','bad':'poor']
            String aq = map.get(raw, raw)
            out << [name:'airQuality', value: aq, descriptionText: "air quality is ${aq}"]
            return
        }

        // ROPO: filter %
        if (code in tuyaFunctions.filter) {
            Integer p = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (p != null) out << [name:'filterLife', value:p, unit:'%', descriptionText:"filter life is ${p}%"]
            return
        }

        // ROPO: UV
        if (code in tuyaFunctions.uv) {
            out << [name:'uv', value: (val ? 'on':'off'), descriptionText:"uv is ${(val?'on':'off')}"]
            return
        }

        // Fault
        if (code in tuyaFunctions.fault) {
            Integer f = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (f != null) out << [name:'fault', value: (f==0?'ok':"fault(${f})"), descriptionText:"fault is ${(f==0?'ok':"fault(${f})")}"]
            return
        }

        
        // Temperature current -> temperature
        if (code in tuyaFunctions.temperature) {
            Integer t = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (t != null) out << [name:'temperature', value:t, unit:'°C', descriptionText:"temperature is ${t}°C"]
            return
        }

        // Heating setpoint -> heatingSetpoint
        if (code in tuyaFunctions.temperatureSet) {
            Integer t = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (t != null) out << [name:'heatingSetpoint', value:t, unit:'°C', descriptionText:"setpoint is ${t}°C"]
            return
        }

        // Work state -> thermostatOperatingState
        if (code in tuyaFunctions.workState) {
            String ws = (val ?: '').toString()
            String op = (ws == 'heating') ? 'heating' : 'idle'
            out << [name:'thermostatOperatingState', value: op, descriptionText:"operating state is ${op}"]
            return
        }

        // Countdown set/left (kept as countdown, NOT converted globally)
        if (code in tuyaFunctions.countdownSet) {
            out << [name:'countdownSet', value: (val ?: '').toString(), descriptionText:"countdown set is ${(val ?: '').toString()}"]
            return
        }

        if (code in tuyaFunctions.countdownLeft) {
            Integer left = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (left != null) out << [name:'countdownLeft', value:left, unit:'min', descriptionText:"countdown left is ${left} min"]
            return
        }
        
                // --- TANK LEVEL MONITOR (ywcgq) ---
        if (code == 'liquid_level_percent') {
            Integer p = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (p != null) out << [name:'level', value: p, unit:'%', descriptionText:"level is ${p}%"]
            return
        }

        if (code == 'liquid_state') {
            String s = (val ?: '').toString()
            out << [name:'liquidState', value: s, descriptionText:"liquid state is ${s}"]
            return
        }

        // liquid_depth: unit m, scale 2 => value / 100
        if (code == 'liquid_depth') {
            BigDecimal m = null
            try {
                if (val instanceof Number) m = (val as BigDecimal) / 100G
                else if (val != null) m = (new BigDecimal(val.toString())) / 100G
            } catch (ignored) { m = null }
            if (m != null) out << [name:'liquidDepth', value: m.setScale(2, BigDecimal.ROUND_HALF_UP), unit:'m', descriptionText:"liquid depth is ${m} m"]
            return
        }

        // max_set / mini_set (percent)
        if (code == 'max_set') {
            Integer p = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (p != null) out << [name:'upperLimit', value:p, unit:'%', descriptionText:"upper limit is ${p}%"]
            return
        }

        if (code == 'mini_set') {
            Integer p = (val instanceof Number) ? (val as Number).intValue() : (val?.toString()?.isInteger() ? val.toString().toInteger() : null)
            if (p != null) out << [name:'lowerLimit', value:p, unit:'%', descriptionText:"lower limit is ${p}%"]
            return
        }

        // installation_height: unit m, scale 3 => value / 1000
        if (code == 'installation_height') {
            BigDecimal m = null
            try {
                if (val instanceof Number) m = (val as BigDecimal) / 1000G
                else if (val != null) m = (new BigDecimal(val.toString())) / 1000G
            } catch (ignored) { m = null }
            if (m != null) out << [name:'installationHeight', value: m.setScale(3, BigDecimal.ROUND_HALF_UP), unit:'m', descriptionText:"installation height is ${m} m"]
            return
        }

        // liquid_depth_max: unit m, scale 3 => value / 1000
        if (code == 'liquid_depth_max') {
            BigDecimal m = null
            try {
                if (val instanceof Number) m = (val as BigDecimal) / 1000G
                else if (val != null) m = (new BigDecimal(val.toString())) / 1000G
            } catch (ignored) { m = null }
            if (m != null) out << [name:'liquidDepthMax', value: m.setScale(3, BigDecimal.ROUND_HALF_UP), unit:'m', descriptionText:"liquid depth max is ${m} m"]
            return
        }

    }

    return out
}

/* =========================================================
 *  MQTT PARSE / STATUS
 * ========================================================= */

void parse(String data) {
    // MQTT packets -> decrypt -> route status
    try {
        Map raw = jsonParser.parseText(interfaces.mqtt.parseMessage(data).payload)
        Cipher cipher = tuyaGetCipher(Cipher.DECRYPT_MODE)
        String decoded = new String(cipher.doFinal(raw.data.decodeBase64()), 'UTF-8')
        Map result = (Map) jsonParser.parseText(decoded)

        if (result?.status != null && (result?.id != null || result?.devId != null)) {
            updateMultiDeviceStatus(result)
        } else if (settings?.logEnable) {
            LOG.debug "MQTT unsupported packet: ${result}"
        }
    } catch (e) {
        LOG.warn "MQTT parse/decrypt error: ${e}"
        sendEvent(name:'state', value:'error', descriptionText:"MQTT parse error")
        runIn(15, 'initialize')
    }
}

void mqttClientStatus(String status) {
    switch (status) {
        case 'Status: Connection succeeded':
            LOG.info "Connected to Tuya MQTT"
            sendEvent(name:'state', value:'connected', descriptionText:'Connected to Tuya MQTT')
            runIn(1, 'tuyaHubSubscribeAsync')
            break
        default:
            LOG.error "MQTT connection error: ${status}"
            sendEvent(name:'state', value:'disconnected', descriptionText:'Disconnected from Tuya MQTT')
            runIn(15, 'initialize')
            break
    }
}

/* =========================================================
 *  AUTH
 * ========================================================= */

private void tuyaAuthenticateAsync() {
    unschedule('tuyaAuthenticateAsync')

    if (!settings?.access_id || !settings?.access_key || !settings?.username || !settings?.password) {
        sendEvent(name:'state', value:'not configured', descriptionText:'Missing settings')
        LOG.error "Missing settings"
        return
    }

    LOG.info "Authenticating to Tuya Cloud"
    sendEvent(name:'state', value:'authenticating', descriptionText:'Authenticating to Tuya')

    state.tokenInfo = [access_token:'', refresh_token:'', uid:'', expire:0L]
    tuyaGetAsync('/v1.0/token', ['grant_type':'1'], 'tuyaAuthenticateResponse', [:])
}

private void tuyaAuthenticateResponse(AsyncResponse response, Map data) {
  String path = data?.path ?: ""
    Integer code = response?.status
    String body = (response?.data instanceof String) ? response.data : (response?.data?.toString() ?: "")

    // (geralmente token não é o device alvo, então não vai imprimir)
    if (isDebugTargetPath(path)) debugDumpResponse(path, response)

    if (!tuyaCheckResponse(response, data)) {
        runIn(15, 'initialize')
        return
    }

    Map r = response.json.result
    state.tokenInfo = [
        access_token : r.access_token,
        refresh_token: r.refresh_token,
        uid          : r.uid,
        expire       : (r.expire_time as Long) * 1000L + now()
    ]

    LOG.info "Authenticated (token valid ${r.expire_time}s)"
    sendEvent(name:'state', value:'authenticated', descriptionText:'Authenticated')

    runIn((int)((r.expire_time as Integer) * 0.90), 'tuyaRefreshTokenAsync')
    tuyaGetHubConfigAsync()
    tuyaGetHomesAsync()
    tuyaGetDevicesAsync()
}

private void tuyaRefreshTokenAsync() {
    unschedule('tuyaRefreshTokenAsync')
    if (!state?.tokenInfo?.refresh_token) {
        tuyaAuthenticateAsync()
        return
    }
    LOG.debug "Refreshing Tuya token"
    tuyaGetAsync("/v1.0/token/${state.tokenInfo.refresh_token}", null, 'tuyaRefreshTokenResponse', [:])
}

private void tuyaRefreshTokenResponse(AsyncResponse response, Map data) {
    if (!tuyaCheckResponse(response, data)) {
        tuyaAuthenticateAsync()
        return
    }
    Map r = response.json.result
    state.tokenInfo = [
        access_token : r.access_token,
        refresh_token: r.refresh_token,
        uid          : r.uid,
        expire       : (r.expire_time as Long) * 1000L + now()
    ]
    LOG.info "Token refreshed"
    sendEvent(name:'state', value:'authenticated', descriptionText:'Token refreshed')
    runIn((int)((r.expire_time as Integer) * 0.90), 'tuyaRefreshTokenAsync')
    tuyaGetHubConfigAsync()
}

/* =========================================================
 *  DEVICES / SPECS
 * ========================================================= */

private void tuyaGetDevicesAsync(String lastRowKey = '', Map data = [:]) {
    LOG.info "Requesting devices batch"
    tuyaGetAsync('/v1.0/iot-01/associated-users/devices', ['last_row_key': lastRowKey], 'tuyaGetDevicesResponse', data)
}

private void tuyaGetDevicesResponse(AsyncResponse response, Map data) {
    if (!tuyaCheckResponse(response, data)) return

    Map r = response.json.result
    data.devices = (data.devices ?: []) + (r.devices ?: [])
    LOG.info "Received ${r.devices?.size() ?: 0} devices (has_more=${r.has_more})"

    if (r.has_more) {
        pauseExecution(500)
        tuyaGetDevicesAsync(r.last_row_key ?: '', data)
        return
    }

    sendEvent(name:'deviceCount', value: (data.devices?.size() ?: 0))
    (data.devices ?: []).each { dev ->
        tuyaGetDeviceSpecificationsAsync(dev.id, dev)
    }
}

private void tuyaGetDeviceSpecificationsAsync(String deviceID, Map data = [:]) {
    LOG.info "Requesting specifications for ${deviceID}"
    tuyaGetAsync("/v1.0/devices/${deviceID}/specifications", null, 'tuyaGetDeviceSpecificationsResponse', data)
}

private void tuyaGetDeviceSpecificationsResponse(AsyncResponse response, Map data) {
    if (isDebugTargetPath(data?.path ?: "")) debugDumpResponse(data.path, response)
    if (!tuyaCheckResponse(response, data)) return

    Map result = response.json.result
    data.category = result.category
    data.functions = [:]
    data.statusSet = [:]

    (result.functions ?: []).each { f ->
        Map values = jsonParser.parseText(f.values ?: '{}')
        values.type = f.type
        data.functions[f.code] = values
    }
    (result.status ?: []).each { s ->
        Map values = jsonParser.parseText(s.values ?: '{}')
        values.type = s.type
        data.statusSet[s.code] = values
    }

    createChildDevices(data)
    tuyaGetStateAsync(data.id)
    sendEvent(name:'state', value:'ready', descriptionText:'Received device data')
}

private void tuyaGetStateAsync(String deviceID) {
    tuyaGetAsync("/v1.0/devices/${deviceID}/status", null, 'tuyaGetStateResponse', [id: deviceID])
}

private void tuyaGetStateResponse(AsyncResponse response, Map data) {
    if (isDebugTargetPath(data?.path ?: "")) debugDumpResponse(data.path, response)
    if (!tuyaCheckResponse(response, data)) return
    data.status = response.json.result ?: []
    updateMultiDeviceStatus(data)
}

/* =========================================================
 *  HOMES / SCENES (optional)
 * ========================================================= */

private void tuyaGetHomesAsync() {
    if (!state?.tokenInfo?.uid) return
    tuyaGetAsync("/v1.0/users/${state.tokenInfo.uid}/homes", null, 'tuyaGetHomesResponse', [:])
}

private void tuyaGetHomesResponse(AsyncResponse response, Map data) {
    if (!tuyaCheckResponse(response, data)) return
    // Not creating scenes here (keep simple/stable)
}

/* =========================================================
 *  COMMANDS
 * ========================================================= */

private void tuyaSendDeviceCommandsAsync(String deviceID, Map... params) {
    if (!state?.tokenInfo?.access_token) {
        LOG.error "Access token missing"
        sendEvent(name:'state', value:'error', descriptionText:'Missing access token')
        runIn(15, 'initialize')
        return
    }
    tuyaPostAsync("/v1.0/devices/${deviceID}/commands", ['commands': params], 'tuyaSendDeviceCommandsResponse', [:])
}

private void tuyaSendDeviceCommandsResponse(AsyncResponse response, Map data) {
    if (tuyaCheckResponse(response, data)) return
    sendEvent(name:'state', value:'error', descriptionText:'Error sending command')
}

/* =========================================================
 *  MQTT HUB CONFIG
 * ========================================================= */

private void tuyaGetHubConfigAsync() {
    if (!state?.tokenInfo?.uid) return
    LOG.info "Requesting MQTT config"
    Map body = [
        uid: state.tokenInfo.uid,
        link_id: state.uuid,
        link_type: 'mqtt',
        topics: 'device',
        msg_encrypted_version: '1.0'
    ]
    tuyaPostAsync('/v1.0/iot-03/open-hub/access-config', body, 'tuyaGetHubConfigResponse', [:])
}

private void tuyaGetHubConfigResponse(AsyncResponse response, Map data) {
    if (!tuyaCheckResponse(response, data)) return

    Map r = response.json.result
    if (!r?.url) {
        LOG.warn "MQTT config missing url"
        return
    }

    state.mqttInfo = r
    tuyaHubConnectAsync()
}

private void tuyaHubConnectAsync() {
    LOG.info "Connecting MQTT ${state.mqttInfo?.url}"
    try {
        interfaces.mqtt.connect(
            state.mqttInfo.url,
            state.mqttInfo.client_id,
            state.mqttInfo.username,
            state.mqttInfo.password
        )
    } catch (e) {
        LOG.error "MQTT connect error: ${e}"
        sendEvent(name:'state', value:'error', descriptionText:'MQTT connect error')
        runIn(15, 'initialize')
    }
}

private void tuyaHubSubscribeAsync() {
    try {
        state.mqttInfo?.source_topic?.each { k, v ->
            LOG.info "Subscribing ${v}"
            interfaces.mqtt.subscribe(v as String)
        }
    } catch (e) {
        LOG.warn "Subscribe error: ${e}"
    }
}

private boolean isDebugTargetPath(String path) {
    String devId = (settings?.debugDumpDeviceId ?: "").trim()
    if (!devId) return false
    // dump apenas para esse device
    return path?.contains("/devices/${devId}/status") || path?.contains("/devices/${devId}/specifications")
}

private void debugDumpResponse(String path, AsyncResponse response) {
    try {
        Integer httpCode = response?.status

        // 1) PRIORIDADE: response.json (Hubitat já parseia)
        def obj = null
        try {
            obj = response?.json
        } catch (ignored) { obj = null }

        // 2) FALLBACK: tentar data como texto
        String bodyText = ""
        try {
            def raw = response?.data
            if (raw instanceof String) bodyText = raw
            else if (raw != null) bodyText = raw.toString()
        } catch (ignored) {}

        String out = ""
        if (obj != null) {
            // Já temos objeto JSON
            out = settings?.debugDumpPretty ? JsonOutput.prettyPrint(JsonOutput.toJson(obj)) : JsonOutput.toJson(obj)
        } else if (bodyText) {
            // Tenta parsear texto
            if (settings?.debugDumpPretty) {
                try {
                    def parsed = new JsonSlurper().parseText(bodyText)
                    out = JsonOutput.prettyPrint(JsonOutput.toJson(parsed))
                } catch (ignored) {
                    out = bodyText
                }
            } else {
                out = bodyText
            }
        }

        log.warn "====== TUYA RAW DEBUG ======"
        log.warn "HTTP ${httpCode} PATH=${path}"
        log.warn(out ? "BODY =>\n${out}" : "BODY => (empty)")
        log.warn "============================"

        // Salvar em state pra copiar depois
        if (settings?.debugDumpSaveState) {
            if (path.contains("/status")) {
                state.lastTuyaStatusHttp = httpCode
                state.lastTuyaStatusJson = out
            } else if (path.contains("/specifications")) {
                state.lastTuyaSpecsHttp = httpCode
                state.lastTuyaSpecsJson = out
            }
        }
    } catch (e) {
        log.warn "DEBUG dump error: ${e}"
    }
}



/* =========================================================
 *  REST HELPERS
 * ========================================================= */

private void tuyaGetAsync(String path, Map query, String callback, Map data) {
    tuyaRequestAsync('get', path, callback, query, null, data)
}

private void tuyaPostAsync(String path, Map body, String callback, Map data) {
    tuyaRequestAsync('post', path, callback, null, body ?: [:], data)
}

private void tuyaRequestAsync(String method, String path, String callback, Map query, Map body, Map data) {
    String accessToken = state?.tokenInfo?.access_token ?: ''
    if (path.startsWith('/v1.0/token')) accessToken = ''

    long nowMs = now()
    String stringToSign = tuyaGetStringToSign(method, path, query, body)

    Map headers = [
        't'                : nowMs,
        'nonce'            : state.uuid,
        'client_id'        : settings.access_id,
        'Signature-Headers': 'client_id',
        'sign'             : tuyaCalculateSignature(accessToken, nowMs, stringToSign),
        'sign_method'      : 'HMAC-SHA256',
        'access_token'     : accessToken,
        'lang'             : state.lang
    ]

    Map request = [
        uri        : state.endPoint,
        path       : path,
        query      : query,
        contentType: 'application/json',
        headers    : headers,
        body       : JsonOutput.toJson(body ?: [:]),
        timeout    : 10
    ]

    if (settings?.logEnable) LOG.debug "API ${method.toUpperCase()} ${path}"

    // GARANTE que o callback sabe qual foi o path/method
    if (data == null) data = [:]
    data.path = path
    data.method = method?.toUpperCase()
    data.query = query
    // não salvar body aqui por segurança; se quiser, só quando debug ativo
    
    if (method == 'get') asynchttpGet(callback, request, data)
    
    else asynchttpPost(callback, request, data)
}

private boolean tuyaCheckResponse(AsyncResponse response, Map data = null) {
    if (response.hasError()) {
        LOG.error "HTTP error: ${response.errorMessage}"
        sendEvent(name:'state', value:'error', descriptionText: response.errorMessage)
        return false
    }
    if (response.status != 200) {
        LOG.error "HTTP status ${response.status}"
        sendEvent(name:'state', value:'error', descriptionText: "HTTP ${response.status}")
        return false
    }
    if (response.json?.success == true) return true

    String p = (data?.path ?: "")

    // Se for o device alvo, dump mesmo em erro (permission deny inclusive)
    if (isDebugTargetPath(p)) {
        debugDumpResponse(p, response)
    }

    String errMsg = response.json?.msg ?: String.valueOf(response.data)
    Integer apiCode = (response.json?.code instanceof Number) ? (response.json.code as Integer) : null

    LOG.error "API fail: ${errMsg}" + (apiCode != null ? " (code=${apiCode})" : "") + (p ? " path=${p}" : "")
	sendEvent(name:'state', value:'error', descriptionText: errMsg)


    // token issues -> re-auth
    switch (response.json?.code) {
        case 1002:
        case 1010:
        case 1011:
        case 1012:
        case 1400:
            tuyaAuthenticateAsync()
            break
    }
    return false
}

private String tuyaCalculateSignature(String accessToken, long timestamp, String stringToSign) {
    String message = settings.access_id + accessToken + timestamp.toString() + state.uuid + stringToSign
    Mac sha256HMAC = Mac.getInstance('HmacSHA256')
    sha256HMAC.init(new SecretKeySpec(settings.access_key.bytes, 'HmacSHA256'))
    return HexUtils.byteArrayToHexString(sha256HMAC.doFinal(message.bytes))
}

private String tuyaGetStringToSign(String method, String path, Map query, Map body) {
    String url = query ? path + '?' + query.sort().collect { k, v -> "${k}=${v}" }.join('&') : path
    String headers = 'client_id:' + settings.access_id + '\n'
    String bodyStream = (body == null) ? '' : JsonOutput.toJson(body)

    MessageDigest sha256 = MessageDigest.getInstance('SHA-256')
    String contentSHA256 = HexUtils.byteArrayToHexString(sha256.digest(bodyStream.bytes)).toLowerCase()

    return method.toUpperCase() + '\n' + contentSHA256 + '\n' + headers + '\n' + url
}

private Cipher tuyaGetCipher(int mode) {
    Cipher cipher = Cipher.getInstance('AES/ECB/PKCS5Padding')
    byte[] cipherKey = (state.mqttInfo?.password ?: '').toString()[8..23].bytes
    cipher.init(mode, new SecretKeySpec(cipherKey, 'AES'))
    return cipher
}

private void logsOff() {
    device.updateSetting('logEnable', [value:'false', type:'bool'])
    LOG.info "Debug logging disabled"
}

/* =========================================================
 *  COUNTRY MAP (reduced)
 * ========================================================= */

private static Map country(String c, String code, String endpoint) {
    [country:c, countryCode:code, endpoint:endpoint]
}

@Field static final List<Map> tuyaCountries = [
    country('Brazil', '55', 'https://openapi.tuyaus.com'),
    country('United States', '1', 'https://openapi.tuyaus.com'),
    country('Portugal', '351', 'https://openapi.tuyaeu.com'),
    country('United Kingdom', '44', 'https://openapi.tuyaeu.com'),
    country('Spain', '34', 'https://openapi.tuyaeu.com'),
    country('France', '33', 'https://openapi.tuyaeu.com'),
    country('Germany', '49', 'https://openapi.tuyaeu.com'),
    country('Italy', '39', 'https://openapi.tuyaeu.com')
]
