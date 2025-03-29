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
import org.elasticsearch.core.Nullable;

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

    private static final Map<String, ExtensionMapping> EXTENSION_MAPPINGS = Map.<String, ExtensionMapping>ofEntries(
        entry("agt", new ExtensionMapping("agentAddress", DataType.IPType, "agent.ip")),
        entry("agentDnsDomain", new ExtensionMapping("agentDnsDomain", DataType.StringType, "agent.name")),
        entry("ahost", new ExtensionMapping("agentHostName", DataType.StringType, "agent.name")),
        entry("aid", new ExtensionMapping("agentId", DataType.StringType, "agent.id")),
        entry("amac", new ExtensionMapping("agentMacAddress", DataType.MACAddressType, "agent.mac")),
        entry("agentNtDomain", new ExtensionMapping("agentNtDomain", DataType.StringType, null)),
        entry("art", new ExtensionMapping("agentReceiptTime", DataType.TimestampType, "event.created")),
        entry("atz", new ExtensionMapping("agentTimeZone", DataType.StringType, null)),
        entry("agentTranslatedAddress", new ExtensionMapping("agentTranslatedAddress", DataType.IPType, null)),
        entry("agentTranslatedZoneExternalID", new ExtensionMapping("agentTranslatedZoneExternalID", DataType.StringType, null)),
        entry("agentTranslatedZoneURI", new ExtensionMapping("agentTranslatedZoneURI", DataType.StringType, null)),
        entry("at", new ExtensionMapping("agentType", DataType.StringType, "agent.type")),
        entry("av", new ExtensionMapping("agentVersion", DataType.StringType, "agent.version")),
        entry("agentZoneExternalID", new ExtensionMapping("agentZoneExternalID", DataType.StringType, null)),
        entry("agentZoneURI", new ExtensionMapping("agentZoneURI", DataType.StringType, null)),
        entry("app", new ExtensionMapping("applicationProtocol", DataType.StringType, "network.protocol")),
        entry("cnt", new ExtensionMapping("baseEventCount", DataType.IntegerType, null)),
        entry("in", new ExtensionMapping("bytesIn", DataType.LongType, "source.bytes")),
        entry("out", new ExtensionMapping("bytesOut", DataType.LongType, "destination.bytes")),
        entry("customerExternalID", new ExtensionMapping("customerExternalID", DataType.StringType, "organization.id")),
        entry("customerURI", new ExtensionMapping("customerURI", DataType.StringType, "organization.name")),
        entry("dst", new ExtensionMapping("destinationAddress", DataType.IPType, "destination.ip")),
        entry("destinationDnsDomain", new ExtensionMapping("destinationDnsDomain", DataType.StringType, "destination.registered_domain")),
        entry("dlat", new ExtensionMapping("destinationGeoLatitude", DataType.DoubleType, "destination.geo.location.lat")),
        entry("dlong", new ExtensionMapping("destinationGeoLongitude", DataType.DoubleType, "destination.geo.location.lon")),
        entry("dhost", new ExtensionMapping("destinationHostName", DataType.StringType, "destination.domain")),
        entry("dmac", new ExtensionMapping("destinationMacAddress", DataType.MACAddressType, "destination.mac")),
        entry("dntdom", new ExtensionMapping("destinationNtDomain", DataType.StringType, "destination.registered_domain")),
        entry("dpt", new ExtensionMapping("destinationPort", DataType.IntegerType, "destination.port")),
        entry("dpid", new ExtensionMapping("destinationProcessId", DataType.LongType, "destination.process.pid")),
        entry("dproc", new ExtensionMapping("destinationProcessName", DataType.StringType, "destination.process.name")),
        entry("destinationServiceName", new ExtensionMapping("destinationServiceName", DataType.StringType, "destination.service.name")),
        entry("destinationTranslatedAddress", new ExtensionMapping("destinationTranslatedAddress", DataType.IPType, "destination.nat.ip")),
        entry("destinationTranslatedPort", new ExtensionMapping("destinationTranslatedPort", DataType.IntegerType, "destination.nat.port")),
        entry(
            "destinationTranslatedZoneExternalID",
            new ExtensionMapping("destinationTranslatedZoneExternalID", DataType.StringType, null)
        ),
        entry("destinationTranslatedZoneURI", new ExtensionMapping("destinationTranslatedZoneURI", DataType.StringType, null)),
        entry("duid", new ExtensionMapping("destinationUserId", DataType.StringType, "destination.user.id")),
        entry("duser", new ExtensionMapping("destinationUserName", DataType.StringType, "destination.user.name")),
        entry("dpriv", new ExtensionMapping("destinationUserPrivileges", DataType.StringType, "destination.user.group.name")),
        entry("destinationZoneExternalID", new ExtensionMapping("destinationZoneExternalID", DataType.StringType, null)),
        entry("destinationZoneURI", new ExtensionMapping("destinationZoneURI", DataType.StringType, null)),
        entry("act", new ExtensionMapping("deviceAction", DataType.StringType, "event.action")),
        entry("dvc", new ExtensionMapping("deviceAddress", DataType.IPType, "observer.ip")),
        entry("cfp1Label", new ExtensionMapping("deviceCustomFloatingPoint1Label", DataType.StringType, null)),
        entry("cfp3Label", new ExtensionMapping("deviceCustomFloatingPoint3Label", DataType.StringType, null)),
        entry("cfp4Label", new ExtensionMapping("deviceCustomFloatingPoint4Label", DataType.StringType, null)),
        entry("deviceCustomDate1", new ExtensionMapping("deviceCustomDate1", DataType.TimestampType, null)),
        entry("deviceCustomDate1Label", new ExtensionMapping("deviceCustomDate1Label", DataType.StringType, null)),
        entry("deviceCustomDate2", new ExtensionMapping("deviceCustomDate2", DataType.TimestampType, null)),
        entry("deviceCustomDate2Label", new ExtensionMapping("deviceCustomDate2Label", DataType.StringType, null)),
        entry("cfp1", new ExtensionMapping("deviceCustomFloatingPoint1", DataType.FloatType, null)),
        entry("cfp2", new ExtensionMapping("deviceCustomFloatingPoint2", DataType.FloatType, null)),
        entry("cfp2Label", new ExtensionMapping("deviceCustomFloatingPoint2Label", DataType.StringType, null)),
        entry("cfp3", new ExtensionMapping("deviceCustomFloatingPoint3", DataType.FloatType, null)),
        entry("cfp4", new ExtensionMapping("deviceCustomFloatingPoint4", DataType.FloatType, null)),
        entry("c6a1", new ExtensionMapping("deviceCustomIPv6Address1", DataType.IPType, null)),
        entry("c6a1Label", new ExtensionMapping("deviceCustomIPv6Address1Label", DataType.StringType, null)),
        entry("c6a2", new ExtensionMapping("deviceCustomIPv6Address2", DataType.IPType, null)),
        entry("c6a2Label", new ExtensionMapping("deviceCustomIPv6Address2Label", DataType.StringType, null)),
        entry("c6a3", new ExtensionMapping("deviceCustomIPv6Address3", DataType.IPType, null)),
        entry("c6a3Label", new ExtensionMapping("deviceCustomIPv6Address3Label", DataType.StringType, null)),
        entry("c6a4", new ExtensionMapping("deviceCustomIPv6Address4", DataType.IPType, null)),
        entry("C6a4Label", new ExtensionMapping("deviceCustomIPv6Address4Label", DataType.StringType, null)),
        entry("cn1", new ExtensionMapping("deviceCustomNumber1", DataType.LongType, null)),
        entry("cn1Label", new ExtensionMapping("deviceCustomNumber1Label", DataType.StringType, null)),
        entry("cn2", new ExtensionMapping("deviceCustomNumber2", DataType.LongType, null)),
        entry("cn2Label", new ExtensionMapping("deviceCustomNumber2Label", DataType.StringType, null)),
        entry("cn3", new ExtensionMapping("deviceCustomNumber3", DataType.LongType, null)),
        entry("cn3Label", new ExtensionMapping("deviceCustomNumber3Label", DataType.StringType, null)),
        entry("cs1", new ExtensionMapping("deviceCustomString1", DataType.StringType, null)),
        entry("cs1Label", new ExtensionMapping("deviceCustomString1Label", DataType.StringType, null)),
        entry("cs2", new ExtensionMapping("deviceCustomString2", DataType.StringType, null)),
        entry("cs2Label", new ExtensionMapping("deviceCustomString2Label", DataType.StringType, null)),
        entry("cs3", new ExtensionMapping("deviceCustomString3", DataType.StringType, null)),
        entry("cs3Label", new ExtensionMapping("deviceCustomString3Label", DataType.StringType, null)),
        entry("cs4", new ExtensionMapping("deviceCustomString4", DataType.StringType, null)),
        entry("cs4Label", new ExtensionMapping("deviceCustomString4Label", DataType.StringType, null)),
        entry("cs5", new ExtensionMapping("deviceCustomString5", DataType.StringType, null)),
        entry("cs5Label", new ExtensionMapping("deviceCustomString5Label", DataType.StringType, null)),
        entry("cs6", new ExtensionMapping("deviceCustomString6", DataType.StringType, null)),
        entry("cs6Label", new ExtensionMapping("deviceCustomString6Label", DataType.StringType, null)),
        entry("deviceDirection", new ExtensionMapping("deviceDirection", DataType.StringType, "network.direction")),
        entry("deviceDnsDomain", new ExtensionMapping("deviceDnsDomain", DataType.StringType, "observer.registered_domain")),
        entry("cat", new ExtensionMapping("deviceEventCategory", DataType.StringType, null)),
        entry("deviceExternalId", new ExtensionMapping("deviceExternalId", DataType.StringType, "observer.name")),
        entry("deviceFacility", new ExtensionMapping("deviceFacility", DataType.LongType, "log.syslog.facility.code")),
        entry("dvchost", new ExtensionMapping("deviceHostName", DataType.StringType, "observer.hostname")),
        entry(
            "deviceInboundInterface",
            new ExtensionMapping("deviceInboundInterface", DataType.StringType, "observer.ingress.interface.name")
        ),
        entry("dvcmac", new ExtensionMapping("deviceMacAddress", DataType.MACAddressType, "observer.mac")),
        entry("deviceNtDomain", new ExtensionMapping("deviceNtDomain", DataType.StringType, null)),
        entry(
            "deviceOutboundInterface",
            new ExtensionMapping("deviceOutboundInterface", DataType.StringType, "observer.egress.interface.name")
        ),
        entry("devicePayloadId", new ExtensionMapping("devicePayloadId", DataType.StringType, "event.id")),
        entry("dvcpid", new ExtensionMapping("deviceProcessId", DataType.LongType, "process.pid")),
        entry("deviceProcessName", new ExtensionMapping("deviceProcessName", DataType.StringType, "process.name")),
        entry("rt", new ExtensionMapping("deviceReceiptTime", DataType.TimestampType, "@timestamp")),
        entry("dtz", new ExtensionMapping("deviceTimeZone", DataType.StringType, "event.timezone")),
        entry("deviceTranslatedAddress", new ExtensionMapping("deviceTranslatedAddress", DataType.IPType, "host.nat.ip")),
        entry("deviceTranslatedZoneExternalID", new ExtensionMapping("deviceTranslatedZoneExternalID", DataType.StringType, null)),
        entry("deviceTranslatedZoneURI", new ExtensionMapping("deviceTranslatedZoneURI", DataType.StringType, null)),
        entry("deviceZoneExternalID", new ExtensionMapping("deviceZoneExternalID", DataType.StringType, null)),
        entry("deviceZoneURI", new ExtensionMapping("deviceZoneURI", DataType.StringType, null)),
        entry("end", new ExtensionMapping("endTime", DataType.TimestampType, "event.end")),
        entry("eventId", new ExtensionMapping("eventId", DataType.StringType, "event.id")),
        entry("outcome", new ExtensionMapping("eventOutcome", DataType.StringType, "event.outcome")),
        entry("externalId", new ExtensionMapping("externalId", DataType.StringType, null)),
        entry("fileCreateTime", new ExtensionMapping("fileCreateTime", DataType.TimestampType, "file.created")),
        entry("fileHash", new ExtensionMapping("fileHash", DataType.StringType, "file.hash")),
        entry("fileId", new ExtensionMapping("fileId", DataType.StringType, "file.inode")),
        entry("fileModificationTime", new ExtensionMapping("fileModificationTime", DataType.TimestampType, "file.mtime")),
        entry("flexNumber1", new ExtensionMapping("deviceFlexNumber1", DataType.LongType, null)),
        entry("flexNumber1Label", new ExtensionMapping("deviceFlexNumber1Label", DataType.StringType, null)),
        entry("flexNumber2", new ExtensionMapping("deviceFlexNumber2", DataType.LongType, null)),
        entry("flexNumber2Label", new ExtensionMapping("deviceFlexNumber2Label", DataType.StringType, null)),
        entry("fname", new ExtensionMapping("filename", DataType.StringType, "file.name")),
        entry("filePath", new ExtensionMapping("filePath", DataType.StringType, "file.path")),
        entry("filePermission", new ExtensionMapping("filePermission", DataType.StringType, "file.group")),
        entry("fsize", new ExtensionMapping("fileSize", DataType.LongType, "file.size")),
        entry("fileType", new ExtensionMapping("fileType", DataType.StringType, "file.type")),
        entry("flexDate1", new ExtensionMapping("flexDate1", DataType.TimestampType, null)),
        entry("flexDate1Label", new ExtensionMapping("flexDate1Label", DataType.StringType, null)),
        entry("flexString1", new ExtensionMapping("flexString1", DataType.StringType, null)),
        entry("flexString2", new ExtensionMapping("flexString2", DataType.StringType, null)),
        entry("flexString1Label", new ExtensionMapping("flexString1Label", DataType.StringType, null)),
        entry("flexString2Label", new ExtensionMapping("flexString2Label", DataType.StringType, null)),
        entry("msg", new ExtensionMapping("message", DataType.StringType, "message")),
        entry("oldFileCreateTime", new ExtensionMapping("oldFileCreateTime", DataType.TimestampType, null)),
        entry("oldFileHash", new ExtensionMapping("oldFileHash", DataType.StringType, null)),
        entry("oldFileId", new ExtensionMapping("oldFileId", DataType.StringType, null)),
        entry("oldFileModificationTime", new ExtensionMapping("oldFileModificationTime", DataType.TimestampType, null)),
        entry("oldFileName", new ExtensionMapping("oldFileName", DataType.StringType, null)),
        entry("oldFilePath", new ExtensionMapping("oldFilePath", DataType.StringType, null)),
        entry("oldFilePermission", new ExtensionMapping("oldFilePermission", DataType.StringType, null)),
        entry("oldFileSize", new ExtensionMapping("oldFileSize", DataType.IntegerType, null)),
        entry("oldFileType", new ExtensionMapping("oldFileType", DataType.StringType, null)),
        entry("rawEvent", new ExtensionMapping("rawEvent", DataType.StringType, "event.original")),
        entry("reason", new ExtensionMapping("Reason", DataType.StringType, "event.reason")),
        entry("requestClientApplication", new ExtensionMapping("requestClientApplication", DataType.StringType, "user_agent.original")),
        entry("requestContext", new ExtensionMapping("requestContext", DataType.StringType, "http.request.referrer")),
        entry("requestCookies", new ExtensionMapping("requestCookies", DataType.StringType, null)),
        entry("requestMethod", new ExtensionMapping("requestMethod", DataType.StringType, "http.request.method")),
        entry("request", new ExtensionMapping("requestUrl", DataType.StringType, "url.original")),
        entry("src", new ExtensionMapping("sourceAddress", DataType.IPType, "source.ip")),
        entry("sourceDnsDomain", new ExtensionMapping("sourceDnsDomain", DataType.StringType, "source.domain")),
        entry("slat", new ExtensionMapping("sourceGeoLatitude", DataType.DoubleType, "source.geo.location.lat")),
        entry("slong", new ExtensionMapping("sourceGeoLongitude", DataType.DoubleType, "source.geo.location.lon")),
        entry("shost", new ExtensionMapping("sourceHostName", DataType.StringType, "source.domain")),
        entry("smac", new ExtensionMapping("sourceMacAddress", DataType.MACAddressType, "source.mac")),
        entry("sntdom", new ExtensionMapping("sourceNtDomain", DataType.StringType, "source.registered_domain")),
        entry("spt", new ExtensionMapping("sourcePort", DataType.IntegerType, "source.port")),
        entry("spid", new ExtensionMapping("sourceProcessId", DataType.LongType, "source.process.pid")),
        entry("sproc", new ExtensionMapping("sourceProcessName", DataType.StringType, "source.process.name")),
        entry("sourceServiceName", new ExtensionMapping("sourceServiceName", DataType.StringType, "source.service.name")),
        entry("sourceTranslatedAddress", new ExtensionMapping("sourceTranslatedAddress", DataType.IPType, "source.nat.ip")),
        entry("sourceTranslatedPort", new ExtensionMapping("sourceTranslatedPort", DataType.IntegerType, "source.nat.port")),
        entry("sourceTranslatedZoneExternalID", new ExtensionMapping("sourceTranslatedZoneExternalID", DataType.StringType, null)),
        entry("sourceTranslatedZoneURI", new ExtensionMapping("sourceTranslatedZoneURI", DataType.StringType, null)),
        entry("suid", new ExtensionMapping("sourceUserId", DataType.StringType, "source.user.id")),
        entry("suser", new ExtensionMapping("sourceUserName", DataType.StringType, "source.user.name")),
        entry("spriv", new ExtensionMapping("sourceUserPrivileges", DataType.StringType, "source.user.group.name")),
        entry("sourceZoneExternalID", new ExtensionMapping("sourceZoneExternalID", DataType.StringType, null)),
        entry("sourceZoneURI", new ExtensionMapping("sourceZoneURI", DataType.StringType, null)),
        entry("start", new ExtensionMapping("startTime", DataType.TimestampType, "event.start")),
        entry("proto", new ExtensionMapping("transportProtocol", DataType.StringType, "network.transport")),
        entry("type", new ExtensionMapping("type", DataType.IntegerType, "event.kind")),
        entry("catdt", new ExtensionMapping("categoryDeviceType", DataType.StringType, null)),
        entry("mrt", new ExtensionMapping("managerReceiptTime", DataType.TimestampType, "event.ingested"))
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
        for (Map.Entry<String, String> entry : extensions.entrySet()) {
            ExtensionMapping mapping = EXTENSION_MAPPINGS.get(entry.getKey());
            if (mapping != null) {
                String ecsKey = mapping.getEcsKey();
                if (ecsKey != null) {
                    // Add the ECS translation to the root of document
                    event.addRootMapping(ecsKey, convertValueToType(entry.getValue(), mapping.getDataType()));
                } else {
                    // Add the extension to the CEF mappings if it doesn't have an ECS translation
                    event.addExtension(mapping.getKey(), convertValueToType(entry.getValue(), mapping.getDataType()));
                }
            } else {
                // Add the extension if the key is not in the mapping
                event.addExtension(entry.getKey(), entry.getValue());
            }
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
        if (type == DataType.LongType) {
            return Long.parseLong(value);
        } else if (type == DataType.DoubleType) {
            return Double.parseDouble(value);
        } else if (type == DataType.FloatType) {
            return Float.parseFloat(value);
        } else if (type == DataType.IntegerType) {
            return Integer.parseInt(value);
        } else if (type == DataType.TimestampType) {
            return toTimestamp(value);
        } else if (type == DataType.MACAddressType) {
            return toMACAddress(value);
        } else if (type == DataType.IPType) {
            return toIP(value);
        } else if (type == DataType.BooleanType) {
            return Boolean.valueOf(value);
        } else {
            return value; // Default string if string type or extension is not defined
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
        Pattern macAddressPattern = Pattern.compile(MAC_ADDRESS_REGEX);
        Matcher matcher = macAddressPattern.matcher(macWithSeparators);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException("Invalid MAC address format");
        }
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
        private final Map<String, Object> cefMappings = new HashMap<>();
        private final Map<String, Object> extensions = new HashMap<>();

        public void addRootMapping(String key, Object value) {
            this.rootMappings.put(key, value);
        }

        public void addCefMapping(String key, Object value) {
            this.cefMappings.put(key, value);
        }

        public void addExtension(String key, Object value) {
            this.extensions.put(key, value);
        }

        public Map<String, Object> getRootMappings() {
            return Collections.unmodifiableMap(rootMappings);
        }

        public Map<String, Object> getCefMappings() {
            cefMappings.put("extensions", extensions);
            return Collections.unmodifiableMap(cefMappings);
        }
    }

    public static class ExtensionMapping {
        private final String key;
        private final DataType dataType;
        @Nullable
        private final String ecsKey;

        ExtensionMapping(String key, DataType dataType, String ecsKey) {
            this.key = key;
            this.dataType = dataType;
            this.ecsKey = ecsKey;
        }

        public String getKey() {
            return key;
        }

        public DataType getDataType() {
            return dataType;
        }

        public String getEcsKey() {
            return ecsKey;
        }
    }
}
