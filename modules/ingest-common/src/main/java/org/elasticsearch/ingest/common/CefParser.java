/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.util.set.Sets;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_DAY;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_DAY;
import static java.util.Map.entry;

final class CefParser {
    private final boolean removeEmptyValue;
    private final ZoneId timezone;

    CefParser(ZoneId timezone, boolean removeEmptyValue) {
        this.removeEmptyValue = removeEmptyValue;
        this.timezone = timezone;
    }

    private static final Pattern HEADER_PATTERN = Pattern.compile("(?:\\\\\\||\\\\\\\\|[^|])*?");
    private static final Pattern HEADER_NEXT_FIELD_PATTERN = Pattern.compile("(" + HEADER_PATTERN.pattern() + ")\\|");
    private static final Pattern HEADER_ESCAPE_CAPTURE = Pattern.compile("\\\\([\\\\|])");

    // New patterns for extension parsing
    private static final String EXTENSION_KEY_PATTERN = "(?:[\\w-]+(?:\\.[^\\.=\\s\\|\\\\\\[\\]]+)*(?:\\[[0-9]+\\])?(?==))";
    private static final Pattern EXTENSION_KEY_ARRAY_CAPTURE = Pattern.compile("^([^\\[\\]]+)((?:\\[[0-9]+\\])+)$");
    private static final String EXTENSION_VALUE_PATTERN = "(?:\\S|\\s(?!" + EXTENSION_KEY_PATTERN + "=))*";
    private static final Pattern EXTENSION_NEXT_KEY_VALUE_PATTERN = Pattern.compile(
        "(" + EXTENSION_KEY_PATTERN + ")=(" + EXTENSION_VALUE_PATTERN + ")(?:\\s+|$)"
    );

    // Comprehensive regex pattern to match various MAC address formats
    public static final String MAC_ADDRESS_REGEX = "^(" +
    // Combined colon and hyphen separated 6-group patterns
        "(([0-9A-Fa-f]{2}[:|-]){5}[0-9A-Fa-f]{2})|" +
        // Dot-separated 6-group pattern
        "([0-9A-Fa-f]{4}\\.){2}[0-9A-Fa-f]{4}|" +
        // Combined colon and hyphen separated 8-group patterns
        "([0-9A-Fa-f]{2}[:|-]){7}[0-9A-Fa-f]{2}|" +
        // Dot-separated EUI-64
        "([0-9A-Fa-f]{4}\\.){3}[0-9A-Fa-f]{4}" + ")$";
    private static final int EUI48_HEX_LENGTH = 48 / 4;
    private static final int EUI64_HEX_LENGTH = 64 / 4;
    private static final int EUI64_HEX_WITH_SEPARATOR_MAX_LENGTH = EUI64_HEX_LENGTH + EUI64_HEX_LENGTH / 2 - 1;
    private static final Map<String, String> EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING = Map.ofEntries(
        entry("\\\\", "\\"),
        entry("\\=", "="),
        entry("\\\n", "\n"),
        entry("\\\r", "\r")
    );

    private static final Map<String, String> FIELD_MAPPINGS = Map.<String, String>ofEntries(
        entry("app", "network.protocol"),
        entry("in", "source.bytes"),
        entry("out", "destination.bytes"),
        entry("dst", "destination.ip"),
        entry("dlat", "destination.geo.location.lat"),
        entry("dlong", "destination.geo.location.lon"),
        entry("dhost", "destination.domain"),
        entry("dmac", "destination.mac"),
        entry("dntdom", "destination.registered_domain"),
        entry("dpt", "destination.port"),
        entry("dpid", "destination.process.pid"),
        entry("dproc", "destination.process.name"),
        entry("duid", "destination.user.id"),
        entry("duser", "destination.user.name"),
        entry("dpriv", "destination.user.group.name"),
        entry("act", "event.action"),
        entry("dvc", "observer.ip"),
        entry("deviceDirection", "network.direction"),
        entry("deviceDnsDomain", "observer.registered_domain"),
        entry("deviceExternalId", "observer.name"),
        entry("deviceFacility", "log.syslog.facility.code"),
        entry("dvchost", "observer.hostname"),
        entry("deviceInboundInterface", "observer.ingress.interface.name"),
        entry("dvcmac", "observer.mac"),
        entry("deviceOutboundInterface", "observer.egress.interface.name"),
        entry("dvcpid", "process.pid"),
        entry("deviceProcessName", "process.name"),
        entry("rt", "@timestamp"),
        entry("dtz", "event.timezone"),
        entry("deviceTranslatedAddress", "host.nat.ip"),
        entry("device.version", "observer.version"),
        entry("deviceVersion", "observer.version"),
        entry("device.product", "observer.product"),
        entry("deviceProduct", "observer.product"),
        entry("device.event_class_id", "event.code"),
        entry("device.vendor", "observer.vendor"),
        entry("deviceVendor", "observer.vendor"),
        entry("end", "event.end"),
        entry("eventId", "event.id"),
        entry("outcome", "event.outcome"),
        entry("fileCreateTime", "file.created"),
        entry("fileHash", "file.hash"),
        entry("fileId", "file.inode"),
        entry("fileModificationTime", "file.mtime"),
        entry("fname", "file.name"),
        entry("filePath", "file.path"),
        entry("filePermission", "file.group"),
        entry("fsize", "file.size"),
        entry("fileType", "file.extension"),
        entry("mrt", "event.ingested"),
        entry("msg", "message"),
        entry("reason", "event.reason"),
        entry("requestClientApplication", "user_agent.original"),
        entry("requestContext", "http.request.referrer"),
        entry("requestMethod", "http.request.method"),
        entry("request", "url.original"),
        entry("src", "source.ip"),
        entry("sourceDnsDomain", "source.registered_domain"),
        entry("slat", "source.geo.location.lat"),
        entry("slong", "source.geo.location.lon"),
        entry("shost", "source.domain"),
        entry("smac", "source.mac"),
        entry("sntdom", "source.registered_domain"),
        entry("spt", "source.port"),
        entry("spid", "source.process.pid"),
        entry("sproc", "source.process.name"),
        entry("sourceServiceName", "source.service.name"),
        entry("suser", "source.user.name"),
        entry("start", "event.start"),
        entry("proto", "network.transport")
    );

    private static final Set<String> FIELD_MAPPINGS_AND_VALUES = Set.copyOf(
        Sets.union(FIELD_MAPPINGS.keySet(), Set.copyOf(FIELD_MAPPINGS.values()))
    );

    private static final Map<String, DataType> FIELDS_WITH_TYPES = Map.<String, DataType>ofEntries(
        entry("@timestamp", DataType.TimestampType),
        entry("destination.bytes", DataType.LongType),
        entry("destination.domain", DataType.StringType),
        entry("destination.geo.location.lat", DataType.DoubleType),
        entry("destination.geo.location.lon", DataType.DoubleType),
        entry("destination.ip", DataType.IPType),
        entry("destination.mac", DataType.MACAddressType),
        entry("destination.port", DataType.LongType),
        entry("destination.process.name", DataType.StringType),
        entry("destination.process.pid", DataType.LongType),
        entry("destination.registered_domain", DataType.StringType),
        entry("destination.user.group.name", DataType.StringType),
        entry("destination.user.id", DataType.StringType),
        entry("destination.user.name", DataType.StringType),
        entry("device.event_class_id", DataType.StringType),
        entry("device.product", DataType.StringType),
        entry("device.vendor", DataType.StringType),
        entry("device.version", DataType.StringType),
        entry("event.action", DataType.StringType),
        entry("event.code", DataType.StringType),
        entry("event.end", DataType.TimestampType),
        entry("event.id", DataType.StringType),
        entry("event.ingested", DataType.TimestampType),
        entry("event.outcome", DataType.StringType),
        entry("event.reason", DataType.StringType),
        entry("event.start", DataType.TimestampType),
        entry("event.timezone", DataType.StringType),
        entry("file.created", DataType.TimestampType),
        entry("file.extension", DataType.StringType),
        entry("file.group", DataType.StringType),
        entry("file.hash", DataType.StringType),
        entry("file.inode", DataType.StringType),
        entry("file.mtime", DataType.TimestampType),
        entry("file.name", DataType.StringType),
        entry("file.path", DataType.StringType),
        entry("file.size", DataType.LongType),
        entry("host.nat.ip", DataType.IPType),
        entry("http.request.method", DataType.StringType),
        entry("http.request.referrer", DataType.StringType),
        entry("log.syslog.facility.code", DataType.LongType),
        entry("message", DataType.StringType),
        entry("network.direction", DataType.StringType),
        entry("network.protocol", DataType.StringType),
        entry("network.transport", DataType.StringType),
        entry("observer.egress.interface.name", DataType.StringType),
        entry("observer.hostname", DataType.StringType),
        entry("observer.ingress.interface.name", DataType.StringType),
        entry("observer.ip", DataType.IPType),
        entry("observer.mac", DataType.MACAddressType),
        entry("observer.name", DataType.StringType),
        entry("observer.registered_domain", DataType.StringType),
        entry("observer.version", DataType.StringType),
        entry("observer.vendor", DataType.StringType),
        entry("observer.product", DataType.StringType),
        entry("process.name", DataType.StringType),
        entry("process.pid", DataType.LongType),
        entry("source.bytes", DataType.LongType),
        entry("source.domain", DataType.StringType),
        entry("source.geo.location.lat", DataType.DoubleType),
        entry("source.geo.location.lon", DataType.DoubleType),
        entry("source.ip", DataType.IPType),
        entry("source.mac", DataType.MACAddressType),
        entry("source.port", DataType.LongType),
        entry("source.process.name", DataType.StringType),
        entry("source.process.pid", DataType.LongType),
        entry("source.registered_domain", DataType.StringType),
        entry("source.service.name", DataType.StringType),
        entry("source.user.name", DataType.StringType),
        entry("url.original", DataType.StringType),
        entry("user_agent.original", DataType.StringType)
    );

    private static final Set<String> ERROR_MESSAGE_INCOMPLETE_CEF_HEADER = Set.of("incomplete CEF header");
    private static final List<String> TIME_LAYOUTS = List.of(
        // MMM dd HH:mm:ss.SSS zzz
        "MMM dd HH:mm:ss.SSS z",
        "MMM dd HH:mm:ss.SSS Z",
        "MMM dd HH:mm:ss.SSS 'GMT'XX:XX",
        // MMM dd HH:mm:sss.SSS
        "MMM dd HH:mm:ss.SSS",
        // MMM dd HH:mm:ss zzz
        "MMM dd HH:mm:ss z",
        "MMM dd HH:mm:ss Z",
        "MMM dd HH:mm:ss 'GMT'XX:XX",
        // MMM dd HH:mm:ss
        "MMM dd HH:mm:ss",
        // MMM dd yyyy HH:mm:ss.SSS zzz
        "MMM dd yyyy HH:mm:ss.SSS z",
        "MMM dd yyyy HH:mm:ss.SSS Z",
        "MMM dd yyyy HH:mm:ss.SSS 'GMT'XX:XX",
        // MMM dd yyyy HH:mm:ss.SSS
        "MMM dd yyyy HH:mm:ss.SSS",
        // MMM dd yyyy HH:mm:ss zzz
        "MMM dd yyyy HH:mm:ss z",
        "MMM dd yyyy HH:mm:ss Z",
        "MMM dd yyyy HH:mm:ss 'GMT'XX:XX",
        // MMM dd yyyy HH:mm:ss
        "MMM dd yyyy HH:mm:ss"
    );

    private final List<ChronoField> CHRONO_FIELDS = List.of(
        NANO_OF_SECOND,
        SECOND_OF_DAY,
        MINUTE_OF_DAY,
        HOUR_OF_DAY,
        DAY_OF_MONTH,
        MONTH_OF_YEAR
    );

    private enum DataType {
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        BooleanType,
        IPType,
        MACAddressType,
        TimestampType;
    }

    CEFEvent process(String cefString) {
        List<String> headers = new ArrayList<>();
        Matcher matcher = HEADER_NEXT_FIELD_PATTERN.matcher(cefString);
        int extensionStart = 0;

        for (int i = 0; i < 7 && matcher.find(); i++) {
            String field = matcher.group(1);
            field = HEADER_ESCAPE_CAPTURE.matcher(field).replaceAll("$1");
            headers.add(field);
            extensionStart = matcher.end();
        }

        if (headers.isEmpty() == false && headers.getFirst().startsWith("CEF:")) {
            CEFEvent event = new CEFEvent();
            // Add error message if there are not enough header fields
            if (headers.size() != 7) {
                event.addRootMapping("error.message", new HashSet<>(ERROR_MESSAGE_INCOMPLETE_CEF_HEADER));
            }
            processHeaders(headers, event);
            processExtensions(cefString, extensionStart, event);
            return event;
        } else {
            throw new IllegalArgumentException("Invalid CEF format");
        }
    }

    private static void processHeaders(List<String> headers, CEFEvent event) {
        for (int i = 0; i < headers.size(); i++) {
            final String value = headers.get(i);
            switch (i) {
                case 0 -> event.addCefMapping("version", value.substring(4));
                case 1 -> {
                    event.addCefMapping("device.vendor", value);
                    event.addRootMapping("observer.vendor", value);
                }
                case 2 -> {
                    event.addCefMapping("device.product", value);
                    event.addRootMapping("observer.product", value);
                }
                case 3 -> {
                    event.addCefMapping("device.version", value);
                    event.addRootMapping("observer.version", value);
                }
                case 4 -> {
                    event.addCefMapping("device.event_class_id", value);
                    event.addRootMapping("event.code", value);
                }
                case 5 -> event.addCefMapping("name", value);
                case 6 -> event.addCefMapping("severity", value);
            }
        }
    }

    private void processExtensions(String cefString, int extensionStart, CEFEvent event) {
        String extensionString = cefString.substring(extensionStart);
        Map<String, String> extensions = parseExtensions(extensionString);

        // Cleanup empty values in extensions
        if (removeEmptyValue) {
            removeEmptyValue(extensions);
        }

        // Translate extensions to possible ECS fields
        Map<String, Object> translatedFields = new HashMap<>();
        for (Map.Entry<String, String> entry : extensions.entrySet()) {
            if (FIELD_MAPPINGS.containsKey(entry.getKey())) {
                String mappedKey = FIELD_MAPPINGS.get(entry.getKey());
                DataType fieldType = FIELDS_WITH_TYPES.get(mappedKey);
                translatedFields.put(mappedKey, convertValueToType(entry.getValue(), fieldType));
            }
        }

        // Add ECS translations to the root of the document
        if (translatedFields.isEmpty() == false) {
            translatedFields.forEach(event::addRootMapping);
        }

        // Remove the translated entries from extensions
        extensions.keySet().removeAll(FIELD_MAPPINGS_AND_VALUES);

        // Add remaining extensions to the event
        for (Map.Entry<String, String> entry : extensions.entrySet()) {
            event.addCefMapping("extensions." + entry.getKey(), entry.getValue());
        }
    }

    private static Map<String, String> parseExtensions(String extensionString) {
        Map<String, String> extensions = new HashMap<>();
        Matcher matcher = EXTENSION_NEXT_KEY_VALUE_PATTERN.matcher(extensionString);
        int lastEnd = 0;
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2);
            // Convert extension field name to strict legal field_reference
            if (key.endsWith("]")) {
                key = convertArrayLikeKey(key);
            }
            extensions.put(key, desanitizeExtensionVal(value.trim()));
            lastEnd = matcher.end();
        }
        // If there's any remaining unparsed content, throw an exception
        if (lastEnd < extensionString.length()) {
            throw new IllegalArgumentException("Invalid extensions; keyless value present: " + extensionString.substring(lastEnd));
        }
        return extensions;
    }

    private Object convertValueToType(String value, DataType type) {
        if (type == DataType.StringType) {
            return value;
        } else if (type == DataType.LongType) {
            return Long.parseLong(value);
        } else if (type == DataType.DoubleType) {
            return Double.parseDouble(value);
        } else if (type == DataType.IntegerType) {
            return Integer.parseInt(value);
        } else if (type == DataType.TimestampType) {
            return toTimestamp(value);
        } else if (type == DataType.MACAddressType) {
            return toMACAddress(value);
        } else if (type == DataType.IPType) {
            return toIP(value);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    // visible for testing
    ZonedDateTime toTimestamp(String value) {
        // First, try parsing as milliseconds
        try {
            long milliseconds = Long.parseLong(value);
            return Instant.ofEpochMilli(milliseconds).atZone(timezone);
        } catch (NumberFormatException ignored) {
            // Not a millisecond timestamp, continue to format parsing
        }
        // Try parsing with different layouts
        for (String layout : TIME_LAYOUTS) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(layout, Locale.ROOT);
                TemporalAccessor accessor = formatter.parse(value);
                // if there is no year nor year-of-era, we fall back to the current one and
                // fill the rest of the date up with the parsed date
                if (accessor.isSupported(ChronoField.YEAR) == false
                    && accessor.isSupported(ChronoField.YEAR_OF_ERA) == false
                    && accessor.isSupported(WeekFields.ISO.weekBasedYear()) == false
                    && accessor.isSupported(WeekFields.of(Locale.ROOT).weekBasedYear()) == false
                    && accessor.isSupported(ChronoField.INSTANT_SECONDS) == false) {
                    int year = LocalDate.now(ZoneOffset.UTC).getYear();
                    ZonedDateTime newTime = Instant.EPOCH.atZone(ZoneOffset.UTC).withYear(year);
                    for (ChronoField field : CHRONO_FIELDS) {
                        if (accessor.isSupported(field)) {
                            newTime = newTime.with(field, accessor.get(field));
                        }
                    }
                    accessor = newTime.withZoneSameLocal(timezone);
                }
                return DateFormatters.from(accessor, Locale.ROOT, timezone).withZoneSameInstant(timezone);
            } catch (DateTimeParseException ignored) {
                // Try next layout
            }
        }
        // If no layout matches, throw an exception
        throw new IllegalArgumentException("Value is not a valid timestamp: " + value);
    }

    String toMACAddress(String v) throws IllegalArgumentException {
        // Insert separators if necessary
        String macWithSeparators = insertMACSeparators(v);

        // Validate MAC address format
        // Compiled pattern for efficient matching
        Pattern macAddressPattern = Pattern.compile(MAC_ADDRESS_REGEX);
        Matcher matcher = macAddressPattern.matcher(macWithSeparators);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException("Invalid MAC address format");
        }
        // Convert to lowercase and return
        return macWithSeparators;
    }

    String toIP(String v) {
        InetAddress address;
        try {
            address = InetAddress.getByName(v);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid IP address format");
        }
        return NetworkAddress.format(address);
    }

    static String insertMACSeparators(String v) {
        // Check that the length is correct for a MAC address without separators.
        // And check that there isn't already a separator in the string.
        if ((v.length() != EUI48_HEX_LENGTH && v.length() != EUI64_HEX_LENGTH)
            || v.charAt(2) == ':'
            || v.charAt(2) == '-'
            || v.charAt(4) == '.') {
            return v;
        }
        StringBuilder sb = new StringBuilder(EUI64_HEX_WITH_SEPARATOR_MAX_LENGTH);
        for (int i = 0; i < v.length(); i++) {
            sb.append(v.charAt(i));
            if (i < v.length() - 1 && i % 2 != 0) {
                sb.append(':');
            }
        }
        return sb.toString();
    }

    private static void removeEmptyValue(Map<String, String> map) {
        map.entrySet().removeIf(entry -> Objects.isNull(entry.getValue()) || entry.getValue().isEmpty());
    }

    private static String convertArrayLikeKey(String key) {
        Matcher matcher = EXTENSION_KEY_ARRAY_CAPTURE.matcher(key);
        if (matcher.matches()) {
            return "[" + matcher.group(1) + "]" + matcher.group(2);
        }
        return key;
    }

    private static String desanitizeExtensionVal(String value) {
        String desanitized = value;
        for (Map.Entry<String, String> entry : EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING.entrySet()) {
            desanitized = desanitized.replace(entry.getKey(), entry.getValue());
        }
        return desanitized;
    }

    public static class CEFEvent {
        private final Map<String, Object> rootMappings = new HashMap<>();
        private final Map<String, String> cefMappings = new HashMap<>();

        public void addRootMapping(String key, Object value) {
            this.rootMappings.put(key, value);
        }

        public void addCefMapping(String key, String value) {
            this.cefMappings.put(key, value);
        }

        public Map<String, Object> getRootMappings() {
            return Collections.unmodifiableMap(rootMappings);
        }

        public Map<String, String> getCefMappings() {
            return Collections.unmodifiableMap(cefMappings);
        }
    }
}
