/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.ingest.IngestDocument;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_DAY;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_DAY;

final class CefParser {
    private final IngestDocument ingestDocument;
    private final boolean removeEmptyValue;
    private final ZoneId timezone;

    CefParser(IngestDocument ingestDocument, ZoneId timezone, boolean removeEmptyValue) {
        this.ingestDocument = ingestDocument;
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
    private static final Map<String, String> EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING = Map.of(
        "\\\\",
        "\\",
        "\\=",
        "=",
        "\\\n",
        "\n",
        "\\\r",
        "\r"
    );
    private static final Map<String, String> FIELD_MAPPING = new HashMap<>();
    private static final Map<String, Class<?>> FIELDS = new HashMap<>();
    private static final Set<String> FILED_MAPPINGS_AND_VALUES = new HashSet<>();

    private static final String ERROR_MESSAGE_INCOMPLETE_CEF_HEADER = "incomplete CEF header";
    private static final List<String> TIME_LAYOUTS = Arrays.asList(
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

    private final List<ChronoField> CHRONO_FIELDS = Arrays.asList(
        NANO_OF_SECOND,
        SECOND_OF_DAY,
        MINUTE_OF_DAY,
        HOUR_OF_DAY,
        DAY_OF_MONTH,
        MONTH_OF_YEAR
    );

    static {
        FIELD_MAPPING.put("app", "network.protocol");
        FIELD_MAPPING.put("in", "source.bytes");
        FIELD_MAPPING.put("out", "destination.bytes");
        FIELD_MAPPING.put("dst", "destination.ip");
        FIELD_MAPPING.put("dlat", "destination.geo.location.lat");
        FIELD_MAPPING.put("dlong", "destination.geo.location.lon");
        FIELD_MAPPING.put("dhost", "destination.domain");
        FIELD_MAPPING.put("dmac", "destination.mac");
        FIELD_MAPPING.put("dntdom", "destination.registered_domain");
        FIELD_MAPPING.put("dpt", "destination.port");
        FIELD_MAPPING.put("dpid", "destination.process.pid");
        FIELD_MAPPING.put("dproc", "destination.process.name");
        FIELD_MAPPING.put("duid", "destination.user.id");
        FIELD_MAPPING.put("duser", "destination.user.name");
        FIELD_MAPPING.put("dpriv", "destination.user.group.name");
        FIELD_MAPPING.put("act", "event.action");
        FIELD_MAPPING.put("dvc", "observer.ip");
        FIELD_MAPPING.put("deviceDirection", "network.direction");
        FIELD_MAPPING.put("deviceDnsDomain", "observer.registered_domain");
        FIELD_MAPPING.put("deviceExternalId", "observer.name");
        FIELD_MAPPING.put("deviceFacility", "log.syslog.facility.code");
        FIELD_MAPPING.put("dvchost", "observer.hostname");
        FIELD_MAPPING.put("deviceInboundInterface", "observer.ingress.interface.name");
        FIELD_MAPPING.put("dvcmac", "observer.mac");
        FIELD_MAPPING.put("deviceOutboundInterface", "observer.egress.interface.name");
        FIELD_MAPPING.put("dvcpid", "process.pid");
        FIELD_MAPPING.put("deviceProcessName", "process.name");
        FIELD_MAPPING.put("rt", "@timestamp");
        FIELD_MAPPING.put("dtz", "event.timezone");
        FIELD_MAPPING.put("deviceTranslatedAddress", "host.nat.ip");
        FIELD_MAPPING.put("device.version", "observer.version");
        FIELD_MAPPING.put("deviceVersion", "observer.version");
        FIELD_MAPPING.put("device.product", "observer.product");
        FIELD_MAPPING.put("deviceProduct", "observer.product");
        FIELD_MAPPING.put("device.event_class_id", "event.code");
        FIELD_MAPPING.put("device.vendor", "observer.vendor");
        FIELD_MAPPING.put("deviceVendor", "observer.vendor");
        FIELD_MAPPING.put("end", "event.end");
        FIELD_MAPPING.put("eventId", "event.id");
        FIELD_MAPPING.put("outcome", "event.outcome");
        FIELD_MAPPING.put("fileCreateTime", "file.created");
        FIELD_MAPPING.put("fileHash", "file.hash");
        FIELD_MAPPING.put("fileId", "file.inode");
        FIELD_MAPPING.put("fileModificationTime", "file.mtime");
        FIELD_MAPPING.put("fname", "file.name");
        FIELD_MAPPING.put("filePath", "file.path");
        FIELD_MAPPING.put("filePermission", "file.group");
        FIELD_MAPPING.put("fsize", "file.size");
        FIELD_MAPPING.put("fileType", "file.extension");
        FIELD_MAPPING.put("mrt", "event.ingested");
        FIELD_MAPPING.put("msg", "message");
        FIELD_MAPPING.put("reason", "event.reason");
        FIELD_MAPPING.put("requestClientApplication", "user_agent.original");
        FIELD_MAPPING.put("requestContext", "http.request.referrer");
        FIELD_MAPPING.put("requestMethod", "http.request.method");
        FIELD_MAPPING.put("request", "url.original");
        FIELD_MAPPING.put("src", "source.ip");
        FIELD_MAPPING.put("sourceDnsDomain", "source.registered_domain");
        FIELD_MAPPING.put("slat", "source.geo.location.lat");
        FIELD_MAPPING.put("slong", "source.geo.location.lon");
        FIELD_MAPPING.put("shost", "source.domain");
        FIELD_MAPPING.put("smac", "source.mac");
        FIELD_MAPPING.put("sntdom", "source.registered_domain");
        FIELD_MAPPING.put("spt", "source.port");
        FIELD_MAPPING.put("spid", "source.process.pid");
        FIELD_MAPPING.put("sproc", "source.process.name");
        FIELD_MAPPING.put("sourceServiceName", "source.service.name");
        FIELD_MAPPING.put("suser", "source.user.name");
        FIELD_MAPPING.put("start", "event.start");
        FIELD_MAPPING.put("proto", "network.transport");
        // Add more mappings as needed

        FIELDS.put("@timestamp", ZonedDateTime.class);
        FIELDS.put("destination.bytes", Long.class);
        FIELDS.put("destination.domain", String.class);
        FIELDS.put("destination.geo.location.lat", Double.class);
        FIELDS.put("destination.geo.location.lon", Double.class);
        FIELDS.put("destination.ip", String.class);
        FIELDS.put("destination.mac", String.class);
        FIELDS.put("destination.port", Long.class);
        FIELDS.put("destination.process.name", String.class);
        FIELDS.put("destination.process.pid", Long.class);
        FIELDS.put("destination.registered_domain", String.class);
        FIELDS.put("destination.user.group.name", String.class);
        FIELDS.put("destination.user.id", String.class);
        FIELDS.put("destination.user.name", String.class);
        FIELDS.put("device.event_class_id", String.class);
        FIELDS.put("device.product", String.class);
        FIELDS.put("device.vendor", String.class);
        FIELDS.put("device.version", String.class);
        FIELDS.put("event.action", String.class);
        FIELDS.put("event.code", String.class);
        FIELDS.put("event.end", ZonedDateTime.class);
        FIELDS.put("event.id", String.class);
        FIELDS.put("event.ingested", ZonedDateTime.class);
        FIELDS.put("event.outcome", String.class);
        FIELDS.put("event.reason", String.class);
        FIELDS.put("event.start", ZonedDateTime.class);
        FIELDS.put("event.timezone", String.class);
        FIELDS.put("file.created", ZonedDateTime.class);
        FIELDS.put("file.extension", String.class);
        FIELDS.put("file.group", String.class);
        FIELDS.put("file.hash", String.class);
        FIELDS.put("file.inode", String.class);
        FIELDS.put("file.mtime", ZonedDateTime.class);
        FIELDS.put("file.name", String.class);
        FIELDS.put("file.path", String.class);
        FIELDS.put("file.size", Long.class);
        FIELDS.put("host.nat.ip", String.class);
        FIELDS.put("http.request.method", String.class);
        FIELDS.put("http.request.referrer", String.class);
        FIELDS.put("log.syslog.facility.code", Long.class);
        FIELDS.put("message", String.class);
        FIELDS.put("network.direction", String.class);
        FIELDS.put("network.protocol", String.class);
        FIELDS.put("network.transport", String.class);
        FIELDS.put("observer.egress.interface.name", String.class);
        FIELDS.put("observer.hostname", String.class);
        FIELDS.put("observer.ingress.interface.name", String.class);
        FIELDS.put("observer.ip", String.class);
        FIELDS.put("observer.mac", String.class);
        FIELDS.put("observer.name", String.class);
        FIELDS.put("observer.registered_domain", String.class);
        FIELDS.put("observer.version", String.class);
        FIELDS.put("observer.vendor", String.class);
        FIELDS.put("observer.product", String.class);
        FIELDS.put("process.name", String.class);
        FIELDS.put("process.pid", Long.class);
        FIELDS.put("source.bytes", Long.class);
        FIELDS.put("source.domain", String.class);
        FIELDS.put("source.geo.location.lat", Double.class);
        FIELDS.put("source.geo.location.lon", Double.class);
        FIELDS.put("source.ip", String.class);
        FIELDS.put("source.mac", String.class);
        FIELDS.put("source.port", Long.class);
        FIELDS.put("source.process.name", String.class);
        FIELDS.put("source.process.pid", Long.class);
        FIELDS.put("source.registered_domain", String.class);
        FIELDS.put("source.service.name", String.class);
        FIELDS.put("source.user.name", String.class);
        FIELDS.put("url.original", String.class);
        FIELDS.put("user_agent.original", String.class);

        FILED_MAPPINGS_AND_VALUES.addAll(FIELD_MAPPING.keySet());
        FILED_MAPPINGS_AND_VALUES.addAll(FIELD_MAPPING.values());
    }

    void process(String cefString, String targetField) {
        List<String> headerFields = new ArrayList<>();
        Matcher headerMatcher = HEADER_NEXT_FIELD_PATTERN.matcher(cefString);
        int extensionStart = 0;

        for (int i = 0; i < 7 && headerMatcher.find(); i++) {
            String field = headerMatcher.group(1);
            field = HEADER_ESCAPE_CAPTURE.matcher(field).replaceAll("$1");
            headerFields.add(field);
            extensionStart = headerMatcher.end();
        }

        if (headerFields.size() > 0 && headerFields.get(0).startsWith("CEF:")) {
            CEFEvent event = new CEFEvent();
            // Add error message if there are not enough header fields
            if (headerFields.size() != 7) {
                ingestDocument.appendFieldValue("error.message", ERROR_MESSAGE_INCOMPLETE_CEF_HEADER);
            }
            for (int i = 0; i < headerFields.size(); i++) {
                switch (i) {
                    case 0 -> event.setVersion(headerFields.get(0).substring(4));
                    case 1 -> {
                        event.setDeviceVendor(headerFields.get(1));
                        ingestDocument.setFieldValue("observer.vendor", headerFields.get(1));
                    }
                    case 2 -> {
                        event.setDeviceProduct(headerFields.get(2));
                        ingestDocument.setFieldValue("observer.product", headerFields.get(2));
                    }
                    case 3 -> {
                        event.setDeviceVersion(headerFields.get(3));
                        ingestDocument.setFieldValue("observer.version", headerFields.get(3));
                    }
                    case 4 -> {
                        event.setDeviceEventClassId(headerFields.get(4));
                        ingestDocument.setFieldValue("event.code", headerFields.get(4));
                    }
                    case 5 -> event.setName(headerFields.get(5));
                    case 6 -> event.setSeverity(headerFields.get(6));
                }
            }
            String extensionString = cefString.substring(extensionStart);
            Map<String, String> extensions = parseExtensions(extensionString);

            if (removeEmptyValue) {
                removeEmptyValue(extensions);
            }
            event.setExtensions(extensions);

            // Translate possible ECS fields and remove them from extensions
            Map<String, Object> translatedFields = extensions.entrySet()
                .stream()
                .filter(entry -> FIELD_MAPPING.containsKey(entry.getKey()))
                .collect(Collectors.toMap(entry -> FIELD_MAPPING.get(entry.getKey()), entry -> {
                    Class<?> fieldType = FIELDS.get(FIELD_MAPPING.get(entry.getKey()));
                    return convertValueToType(entry.getValue(), fieldType);
                }));

            // Remove the translated entries from extensions
            event.removeMappedExtensions();

            ingestDocument.setFieldValue(targetField, event.toObject());
            // Add ECS translations to the root of the document
            if (translatedFields.isEmpty() == false) {
                translatedFields.forEach(ingestDocument::setFieldValue);
            }
        } else {
            throw new IllegalArgumentException("Invalid CEF format");
        }
    }

    private Object convertValueToType(String value, Class<?> type) {
        if (type == Long.class) {
            return Long.parseLong(value);
        } else if (type == Double.class) {
            return Double.parseDouble(value);
        } else if (type == Integer.class) {
            return Integer.parseInt(value);
        } else if (type == ZonedDateTime.class) {
            return toTimestamp(value);
        } else {
            return value; // Default to String
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

    private static void removeEmptyValue(Map<String, String> map) {
        map.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());
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
        private String version;
        private String deviceVendor;
        private String deviceProduct;
        private String deviceVersion;
        private String deviceEventClassId;
        private String name;
        private String severity;
        private final Map<String, String> extensions = new HashMap<>();
        private Map<String, String> translatedFields;

        // Getters and setters for all fields
        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getDeviceVendor() {
            return deviceVendor;
        }

        public void setDeviceVendor(String deviceVendor) {
            this.deviceVendor = deviceVendor;
        }

        public String getDeviceProduct() {
            return deviceProduct;
        }

        public void setDeviceProduct(String deviceProduct) {
            this.deviceProduct = deviceProduct;
        }

        public String getDeviceVersion() {
            return deviceVersion;
        }

        public void setDeviceVersion(String deviceVersion) {
            this.deviceVersion = deviceVersion;
        }

        public String getDeviceEventClassId() {
            return deviceEventClassId;
        }

        public void setDeviceEventClassId(String deviceEventClassId) {
            this.deviceEventClassId = deviceEventClassId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSeverity() {
            return severity;
        }

        public void setSeverity(String severity) {
            this.severity = severity;
        }

        public Map<String, String> getExtensions() {
            return extensions;
        }

        public void setExtensions(Map<String, String> extensions) {
            this.extensions.putAll(extensions);
        }

        public void addExtension(String key, String extension) {
            this.extensions.put(key, extension);
        }

        public void removeMappedExtensions() {
            extensions.entrySet().removeIf(entry -> FILED_MAPPINGS_AND_VALUES.contains(entry.getKey()));
        }

        public Map<String, String> getTranslatedFields() {
            return translatedFields;
        }

        public void setTranslatedFields(Map<String, String> translatedFields) {
            this.translatedFields = translatedFields;
        }

        public Object toObject() {
            Map<String, Object> event = new HashMap<>();
            event.put("version", version);
            event.put("device.vendor", deviceVendor);
            event.put("device.product", deviceProduct);
            event.put("device.version", deviceVersion);
            event.put("device.event_class_id", deviceEventClassId);
            event.put("name", name);
            event.put("severity", severity);
            for (Map.Entry<String, String> entry : extensions.entrySet()) {
                event.put("extensions." + entry.getKey(), entry.getValue());
            }
            return event;
        }
    }
}
