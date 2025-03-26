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
import org.elasticsearch.common.util.set.Sets;
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
import static java.util.Map.entry;

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
    private static final Map<String, String> EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING = Map.ofEntries(
        entry("\\\\", "\\"),
        entry("\\=", "="),
        entry("\\\n", "\n"),
        entry("\\\r", "\r")
    );

    private static final Map<String, String> FIELD_MAPPING = Map.<String, String>ofEntries(
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

    private static final Map<String, Class<?>> FIELDS = Map.<String, Class<?>>ofEntries(
        entry("@timestamp", ZonedDateTime.class),
        entry("destination.bytes", Long.class),
        entry("destination.domain", String.class),
        entry("destination.geo.location.lat", Double.class),
        entry("destination.geo.location.lon", Double.class),
        entry("destination.ip", String.class),
        entry("destination.mac", String.class),
        entry("destination.port", Long.class),
        entry("destination.process.name", String.class),
        entry("destination.process.pid", Long.class),
        entry("destination.registered_domain", String.class),
        entry("destination.user.group.name", String.class),
        entry("destination.user.id", String.class),
        entry("destination.user.name", String.class),
        entry("device.event_class_id", String.class),
        entry("device.product", String.class),
        entry("device.vendor", String.class),
        entry("device.version", String.class),
        entry("event.action", String.class),
        entry("event.code", String.class),
        entry("event.end", ZonedDateTime.class),
        entry("event.id", String.class),
        entry("event.ingested", ZonedDateTime.class),
        entry("event.outcome", String.class),
        entry("event.reason", String.class),
        entry("event.start", ZonedDateTime.class),
        entry("event.timezone", String.class),
        entry("file.created", ZonedDateTime.class),
        entry("file.extension", String.class),
        entry("file.group", String.class),
        entry("file.hash", String.class),
        entry("file.inode", String.class),
        entry("file.mtime", ZonedDateTime.class),
        entry("file.name", String.class),
        entry("file.path", String.class),
        entry("file.size", Long.class),
        entry("host.nat.ip", String.class),
        entry("http.request.method", String.class),
        entry("http.request.referrer", String.class),
        entry("log.syslog.facility.code", Long.class),
        entry("message", String.class),
        entry("network.direction", String.class),
        entry("network.protocol", String.class),
        entry("network.transport", String.class),
        entry("observer.egress.interface.name", String.class),
        entry("observer.hostname", String.class),
        entry("observer.ingress.interface.name", String.class),
        entry("observer.ip", String.class),
        entry("observer.mac", String.class),
        entry("observer.name", String.class),
        entry("observer.registered_domain", String.class),
        entry("observer.version", String.class),
        entry("observer.vendor", String.class),
        entry("observer.product", String.class),
        entry("process.name", String.class),
        entry("process.pid", Long.class),
        entry("source.bytes", Long.class),
        entry("source.domain", String.class),
        entry("source.geo.location.lat", Double.class),
        entry("source.geo.location.lon", Double.class),
        entry("source.ip", String.class),
        entry("source.mac", String.class),
        entry("source.port", Long.class),
        entry("source.process.name", String.class),
        entry("source.process.pid", Long.class),
        entry("source.registered_domain", String.class),
        entry("source.service.name", String.class),
        entry("source.user.name", String.class),
        entry("url.original", String.class),
        entry("user_agent.original", String.class)
    );

    private static final Set<String> FIELD_MAPPINGS_AND_VALUES = Set.copyOf(
        Sets.union(FIELD_MAPPING.keySet(), Set.copyOf(FIELD_MAPPING.values()))
    );

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

    private final List<ChronoField> CHRONO_FIELDS = List.of(
        NANO_OF_SECOND,
        SECOND_OF_DAY,
        MINUTE_OF_DAY,
        HOUR_OF_DAY,
        DAY_OF_MONTH,
        MONTH_OF_YEAR
    );

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
            this.extensions.keySet().removeAll(FIELD_MAPPINGS_AND_VALUES);
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
