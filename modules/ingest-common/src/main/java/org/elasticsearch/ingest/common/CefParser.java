/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.common;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.Nullable;

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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_DAY;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_DAY;
import static java.util.Map.entry;
import static org.elasticsearch.ingest.common.CefParser.DataType.DoubleType;
import static org.elasticsearch.ingest.common.CefParser.DataType.IPType;
import static org.elasticsearch.ingest.common.CefParser.DataType.IntegerType;
import static org.elasticsearch.ingest.common.CefParser.DataType.LongType;
import static org.elasticsearch.ingest.common.CefParser.DataType.MACAddressType;
import static org.elasticsearch.ingest.common.CefParser.DataType.StringType;
import static org.elasticsearch.ingest.common.CefParser.DataType.TimestampType;

final class CefParser {
    private final boolean removeEmptyValues;
    private final ZoneId timezone;

    CefParser(ZoneId timezone, boolean removeEmptyValues) {
        this.removeEmptyValues = removeEmptyValues;
        this.timezone = timezone;
    }

    // New patterns for extension parsing
    private static final String EXTENSION_KEY_PATTERN = "(?:[\\w-]+(?:\\.[^\\.=\\s\\|\\\\\\[\\]]+)*(?:\\[[0-9]+\\])?(?==))";
    private static final String EXTENSION_VALUE_PATTERN = "(?:[^\\s\\\\]|\\\\[^|]|\\s(?!" + EXTENSION_KEY_PATTERN + "=))*";
    private static final Pattern EXTENSION_NEXT_KEY_VALUE_PATTERN = Pattern.compile(
        "(" + EXTENSION_KEY_PATTERN + ")=(" + EXTENSION_VALUE_PATTERN + ")(?:\\s+|$)"
    );

    // Comprehensive regex pattern to match various MAC address formats
    private static final List<String> MAC_ADDRESS_REGEXES = List.of(
        // Combined colon and hyphen separated 6-group patterns
        "(([0-9A-Fa-f]{2}[:|-]){5}[0-9A-Fa-f]{2})",
        // Dot-separated 6-group pattern
        "([0-9A-Fa-f]{4}\\.){2}[0-9A-Fa-f]{4}",
        // Combined colon and hyphen separated 8-group patterns
        "([0-9A-Fa-f]{2}[:|-]){7}[0-9A-Fa-f]{2}",
        // Dot-separated EUI-64
        "([0-9A-Fa-f]{4}\\.){3}[0-9A-Fa-f]{4}"
    );

    private static final Pattern MAC_ADDRESS_PATTERN = Pattern.compile(
        MAC_ADDRESS_REGEXES.stream().collect(Collectors.joining("|", "^(", ")$"))
    );
    private static final int EUI48_HEX_LENGTH = 48 / 4;
    private static final int EUI64_HEX_LENGTH = 64 / 4;
    private static final int EUI64_HEX_WITH_SEPARATOR_MAX_LENGTH = EUI64_HEX_LENGTH + EUI64_HEX_LENGTH / 2 - 1;
    private static final Map<String, String> EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING = Map.ofEntries(
        entry("\\\\", "\\"),
        entry("\\=", "="),
        entry("\\\n", "\n"),
        entry("\\\r", "\r")
    );

    enum DataType {
        IntegerType,
        LongType,
        DoubleType,
        StringType,
        IPType,
        MACAddressType,
        TimestampType
    }

    private static final Map<String, ExtensionMapping> EXTENSION_MAPPINGS = Map.<String, ExtensionMapping>ofEntries(
        entry("agt", new ExtensionMapping("agentAddress", IPType, "agent.ip")),
        entry("agentDnsDomain", new ExtensionMapping("agentDnsDomain", StringType, "agent.name")),
        entry("ahost", new ExtensionMapping("agentHostName", StringType, "agent.name")),
        entry("aid", new ExtensionMapping("agentId", StringType, "agent.id")),
        entry("amac", new ExtensionMapping("agentMacAddress", MACAddressType, "agent.mac")),
        entry("agentNtDomain", new ExtensionMapping("agentNtDomain", StringType, null)),
        entry("art", new ExtensionMapping("agentReceiptTime", TimestampType, "event.created")),
        entry("atz", new ExtensionMapping("agentTimeZone", StringType, null)),
        entry("agentTranslatedAddress", new ExtensionMapping("agentTranslatedAddress", IPType, null)),
        entry("agentTranslatedZoneExternalID", new ExtensionMapping("agentTranslatedZoneExternalID", StringType, null)),
        entry("agentTranslatedZoneURI", new ExtensionMapping("agentTranslatedZoneURI", StringType, null)),
        entry("at", new ExtensionMapping("agentType", StringType, "agent.type")),
        entry("av", new ExtensionMapping("agentVersion", StringType, "agent.version")),
        entry("agentZoneExternalID", new ExtensionMapping("agentZoneExternalID", StringType, null)),
        entry("agentZoneURI", new ExtensionMapping("agentZoneURI", StringType, null)),
        entry("app", new ExtensionMapping("applicationProtocol", StringType, "network.protocol")),
        entry("cnt", new ExtensionMapping("baseEventCount", IntegerType, null)),
        entry("in", new ExtensionMapping("bytesIn", LongType, "source.bytes")), // LongType from Spec 1.x
        entry("out", new ExtensionMapping("bytesOut", LongType, "destination.bytes")), // LongType from Spec 1.x
        entry("customerExternalID", new ExtensionMapping("customerExternalID", StringType, "organization.id")),
        entry("customerURI", new ExtensionMapping("customerURI", StringType, "organization.name")),
        entry("dst", new ExtensionMapping("destinationAddress", IPType, "destination.ip")),
        entry("destinationDnsDomain", new ExtensionMapping("destinationDnsDomain", StringType, "destination.registered_domain")),
        entry("dlat", new ExtensionMapping("destinationGeoLatitude", DoubleType, "destination.geo.location.lat")),
        entry("dlong", new ExtensionMapping("destinationGeoLongitude", DoubleType, "destination.geo.location.lon")),
        entry("dhost", new ExtensionMapping("destinationHostName", StringType, "destination.domain")),
        entry("dmac", new ExtensionMapping("destinationMacAddress", MACAddressType, "destination.mac")),
        entry("dntdom", new ExtensionMapping("destinationNtDomain", StringType, "destination.registered_domain")),
        entry("dpt", new ExtensionMapping("destinationPort", IntegerType, "destination.port")),
        entry("dpid", new ExtensionMapping("destinationProcessId", IntegerType, "destination.process.pid")),
        entry("dproc", new ExtensionMapping("destinationProcessName", StringType, "destination.process.name")),
        entry("destinationServiceName", new ExtensionMapping("destinationServiceName", StringType, "destination.service.name")),
        entry("destinationTranslatedAddress", new ExtensionMapping("destinationTranslatedAddress", IPType, "destination.nat.ip")),
        entry("destinationTranslatedPort", new ExtensionMapping("destinationTranslatedPort", IntegerType, "destination.nat.port")),
        entry("destinationTranslatedZoneExternalID", new ExtensionMapping("destinationTranslatedZoneExternalID", StringType, null)),
        entry("destinationTranslatedZoneURI", new ExtensionMapping("destinationTranslatedZoneURI", StringType, null)),
        entry("duid", new ExtensionMapping("destinationUserId", StringType, "destination.user.id")),
        entry("duser", new ExtensionMapping("destinationUserName", StringType, "destination.user.name")),
        entry("dpriv", new ExtensionMapping("destinationUserPrivileges", StringType, "destination.user.group.name")),
        entry("destinationZoneExternalID", new ExtensionMapping("destinationZoneExternalID", StringType, null)),
        entry("destinationZoneURI", new ExtensionMapping("destinationZoneURI", StringType, null)),
        entry("act", new ExtensionMapping("deviceAction", StringType, "event.action")),
        entry("dvc", new ExtensionMapping("deviceAddress", IPType, "observer.ip")),
        entry("cfp1Label", new ExtensionMapping("deviceCustomFloatingPoint1Label", StringType, null)),
        entry("cfp3Label", new ExtensionMapping("deviceCustomFloatingPoint3Label", StringType, null)),
        entry("cfp4Label", new ExtensionMapping("deviceCustomFloatingPoint4Label", StringType, null)),
        entry("deviceCustomDate1", new ExtensionMapping("deviceCustomDate1", TimestampType, null)),
        entry("deviceCustomDate1Label", new ExtensionMapping("deviceCustomDate1Label", StringType, null)),
        entry("deviceCustomDate2", new ExtensionMapping("deviceCustomDate2", TimestampType, null)),
        entry("deviceCustomDate2Label", new ExtensionMapping("deviceCustomDate2Label", StringType, null)),
        entry("cfp1", new ExtensionMapping("deviceCustomFloatingPoint1", DoubleType, null)),
        entry("cfp2", new ExtensionMapping("deviceCustomFloatingPoint2", DoubleType, null)),
        entry("cfp2Label", new ExtensionMapping("deviceCustomFloatingPoint2Label", StringType, null)),
        entry("cfp3", new ExtensionMapping("deviceCustomFloatingPoint3", DoubleType, null)),
        entry("cfp4", new ExtensionMapping("deviceCustomFloatingPoint4", DoubleType, null)),
        entry("c6a1", new ExtensionMapping("deviceCustomIPv6Address1", IPType, null)),
        entry("c6a1Label", new ExtensionMapping("deviceCustomIPv6Address1Label", StringType, null)),
        entry("c6a2", new ExtensionMapping("deviceCustomIPv6Address2", IPType, null)),
        entry("c6a2Label", new ExtensionMapping("deviceCustomIPv6Address2Label", StringType, null)),
        entry("c6a3", new ExtensionMapping("deviceCustomIPv6Address3", IPType, null)),
        entry("c6a3Label", new ExtensionMapping("deviceCustomIPv6Address3Label", StringType, null)),
        entry("c6a4", new ExtensionMapping("deviceCustomIPv6Address4", IPType, null)),
        entry("c6a4Label", new ExtensionMapping("deviceCustomIPv6Address4Label", StringType, null)),
        entry("cn1", new ExtensionMapping("deviceCustomNumber1", LongType, null)),
        entry("cn1Label", new ExtensionMapping("deviceCustomNumber1Label", StringType, null)),
        entry("cn2", new ExtensionMapping("deviceCustomNumber2", LongType, null)),
        entry("cn2Label", new ExtensionMapping("deviceCustomNumber2Label", StringType, null)),
        entry("cn3", new ExtensionMapping("deviceCustomNumber3", LongType, null)),
        entry("cn3Label", new ExtensionMapping("deviceCustomNumber3Label", StringType, null)),
        entry("cs1", new ExtensionMapping("deviceCustomString1", StringType, null)),
        entry("cs1Label", new ExtensionMapping("deviceCustomString1Label", StringType, null)),
        entry("cs2", new ExtensionMapping("deviceCustomString2", StringType, null)),
        entry("cs2Label", new ExtensionMapping("deviceCustomString2Label", StringType, null)),
        entry("cs3", new ExtensionMapping("deviceCustomString3", StringType, null)),
        entry("cs3Label", new ExtensionMapping("deviceCustomString3Label", StringType, null)),
        entry("cs4", new ExtensionMapping("deviceCustomString4", StringType, null)),
        entry("cs4Label", new ExtensionMapping("deviceCustomString4Label", StringType, null)),
        entry("cs5", new ExtensionMapping("deviceCustomString5", StringType, null)),
        entry("cs5Label", new ExtensionMapping("deviceCustomString5Label", StringType, null)),
        entry("cs6", new ExtensionMapping("deviceCustomString6", StringType, null)),
        entry("cs6Label", new ExtensionMapping("deviceCustomString6Label", StringType, null)),
        entry("deviceDirection", new ExtensionMapping("deviceDirection", StringType, "network.direction")),
        entry("deviceDnsDomain", new ExtensionMapping("deviceDnsDomain", StringType, "observer.registered_domain")),
        entry("cat", new ExtensionMapping("deviceEventCategory", StringType, null)),
        entry("deviceExternalId", new ExtensionMapping("deviceExternalId", StringType, "observer.name")),
        entry("deviceFacility", new ExtensionMapping("deviceFacility", StringType, null)),
        entry("dvchost", new ExtensionMapping("deviceHostName", StringType, "observer.hostname")),
        entry("deviceInboundInterface", new ExtensionMapping("deviceInboundInterface", StringType, "observer.ingress.interface.name")),
        entry("dvcmac", new ExtensionMapping("deviceMacAddress", MACAddressType, "observer.mac")),
        entry("deviceNtDomain", new ExtensionMapping("deviceNtDomain", StringType, null)),
        entry("deviceOutboundInterface", new ExtensionMapping("deviceOutboundInterface", StringType, "observer.egress.interface.name")),
        entry("devicePayloadId", new ExtensionMapping("devicePayloadId", StringType, "event.id")),
        entry("dvcpid", new ExtensionMapping("deviceProcessId", IntegerType, "process.pid")),
        entry("deviceProcessName", new ExtensionMapping("deviceProcessName", StringType, "process.name")),
        entry("rt", new ExtensionMapping("deviceReceiptTime", TimestampType, "@timestamp")),
        entry("dtz", new ExtensionMapping("deviceTimeZone", StringType, "event.timezone")),
        entry("deviceTranslatedAddress", new ExtensionMapping("deviceTranslatedAddress", IPType, "host.nat.ip")),
        entry("deviceTranslatedZoneExternalID", new ExtensionMapping("deviceTranslatedZoneExternalID", StringType, null)),
        entry("deviceTranslatedZoneURI", new ExtensionMapping("deviceTranslatedZoneURI", StringType, null)),
        entry("deviceZoneExternalID", new ExtensionMapping("deviceZoneExternalID", StringType, null)),
        entry("deviceZoneURI", new ExtensionMapping("deviceZoneURI", StringType, null)),
        entry("end", new ExtensionMapping("endTime", TimestampType, "event.end")),
        entry("eventId", new ExtensionMapping("eventId", StringType, "event.id")),
        entry("outcome", new ExtensionMapping("eventOutcome", StringType, "event.outcome")),
        entry("externalId", new ExtensionMapping("externalId", StringType, null)),
        entry("fileCreateTime", new ExtensionMapping("fileCreateTime", TimestampType, "file.created")),
        entry("fileHash", new ExtensionMapping("fileHash", StringType, "file.hash")),
        entry("fileId", new ExtensionMapping("fileId", StringType, "file.inode")),
        entry("fileModificationTime", new ExtensionMapping("fileModificationTime", TimestampType, "file.mtime")),
        entry("flexNumber1", new ExtensionMapping("deviceFlexNumber1", LongType, null)),
        entry("flexNumber1Label", new ExtensionMapping("deviceFlexNumber1Label", StringType, null)),
        entry("flexNumber2", new ExtensionMapping("deviceFlexNumber2", LongType, null)),
        entry("flexNumber2Label", new ExtensionMapping("deviceFlexNumber2Label", StringType, null)),
        entry("fname", new ExtensionMapping("filename", StringType, "file.name")),
        entry("filePath", new ExtensionMapping("filePath", StringType, "file.path")),
        entry("filePermission", new ExtensionMapping("filePermission", StringType, "file.group")),
        entry("fsize", new ExtensionMapping("fileSize", LongType, "file.size")),
        entry("fileType", new ExtensionMapping("fileType", StringType, "file.type")),
        entry("flexDate1", new ExtensionMapping("flexDate1", TimestampType, null)),
        entry("flexDate1Label", new ExtensionMapping("flexDate1Label", StringType, null)),
        entry("flexString1", new ExtensionMapping("flexString1", StringType, null)),
        entry("flexString2", new ExtensionMapping("flexString2", StringType, null)),
        entry("flexString1Label", new ExtensionMapping("flexString1Label", StringType, null)),
        entry("flexString2Label", new ExtensionMapping("flexString2Label", StringType, null)),
        entry("msg", new ExtensionMapping("message", StringType, "message")),
        entry("oldFileCreateTime", new ExtensionMapping("oldFileCreateTime", TimestampType, null)),
        entry("oldFileHash", new ExtensionMapping("oldFileHash", StringType, null)),
        entry("oldFileId", new ExtensionMapping("oldFileId", StringType, null)),
        entry("oldFileModificationTime", new ExtensionMapping("oldFileModificationTime", TimestampType, null)),
        entry("oldFileName", new ExtensionMapping("oldFileName", StringType, null)),
        entry("oldFilePath", new ExtensionMapping("oldFilePath", StringType, null)),
        entry("oldFilePermission", new ExtensionMapping("oldFilePermission", StringType, null)),
        entry("oldFileSize", new ExtensionMapping("oldFileSize", LongType, null)),
        entry("oldFileType", new ExtensionMapping("oldFileType", StringType, null)),
        entry("rawEvent", new ExtensionMapping("rawEvent", StringType, "event.original")),
        entry("reason", new ExtensionMapping("reason", StringType, "event.reason")),
        entry("requestClientApplication", new ExtensionMapping("requestClientApplication", StringType, "user_agent.original")),
        entry("requestContext", new ExtensionMapping("requestContext", StringType, "http.request.referrer")),
        entry("requestCookies", new ExtensionMapping("requestCookies", StringType, null)),
        entry("requestMethod", new ExtensionMapping("requestMethod", StringType, "http.request.method")),
        entry("request", new ExtensionMapping("requestUrl", StringType, "url.original")),
        entry("src", new ExtensionMapping("sourceAddress", IPType, "source.ip")),
        entry("sourceDnsDomain", new ExtensionMapping("sourceDnsDomain", StringType, "source.domain")),
        entry("slat", new ExtensionMapping("sourceGeoLatitude", DoubleType, "source.geo.location.lat")),
        entry("slong", new ExtensionMapping("sourceGeoLongitude", DoubleType, "source.geo.location.lon")),
        entry("shost", new ExtensionMapping("sourceHostName", StringType, "source.domain")),
        entry("smac", new ExtensionMapping("sourceMacAddress", MACAddressType, "source.mac")),
        entry("sntdom", new ExtensionMapping("sourceNtDomain", StringType, "source.registered_domain")),
        entry("spt", new ExtensionMapping("sourcePort", IntegerType, "source.port")),
        entry("spid", new ExtensionMapping("sourceProcessId", IntegerType, "source.process.pid")),
        entry("sproc", new ExtensionMapping("sourceProcessName", StringType, "source.process.name")),
        entry("sourceServiceName", new ExtensionMapping("sourceServiceName", StringType, "source.service.name")),
        entry("sourceTranslatedAddress", new ExtensionMapping("sourceTranslatedAddress", IPType, "source.nat.ip")),
        entry("sourceTranslatedPort", new ExtensionMapping("sourceTranslatedPort", IntegerType, "source.nat.port")),
        entry("sourceTranslatedZoneExternalID", new ExtensionMapping("sourceTranslatedZoneExternalID", StringType, null)),
        entry("sourceTranslatedZoneURI", new ExtensionMapping("sourceTranslatedZoneURI", StringType, null)),
        entry("suid", new ExtensionMapping("sourceUserId", StringType, "source.user.id")),
        entry("suser", new ExtensionMapping("sourceUserName", StringType, "source.user.name")),
        entry("spriv", new ExtensionMapping("sourceUserPrivileges", StringType, "source.user.group.name")),
        entry("sourceZoneExternalID", new ExtensionMapping("sourceZoneExternalID", StringType, null)),
        entry("sourceZoneURI", new ExtensionMapping("sourceZoneURI", StringType, null)),
        entry("start", new ExtensionMapping("startTime", TimestampType, "event.start")),
        entry("proto", new ExtensionMapping("transportProtocol", StringType, "network.transport")),
        entry("type", new ExtensionMapping("type", StringType, "event.kind")),
        entry("catdt", new ExtensionMapping("categoryDeviceType", StringType, null)),
        entry("mrt", new ExtensionMapping("managerReceiptTime", TimestampType, "event.ingested")),
        // CEF Spec version 1.2
        entry("agentTranslatedZoneKey", new ExtensionMapping("agentTranslatedZoneKey", LongType, null)),
        entry("agentZoneKey", new ExtensionMapping("agentZoneKey", LongType, null)),
        entry("customerKey", new ExtensionMapping("customerKey", LongType, null)),
        entry("destinationTranslatedZoneKey", new ExtensionMapping("destinationTranslatedZoneKey", LongType, null)),
        entry("dZoneKey", new ExtensionMapping("destinationZoneKey", LongType, null)),
        entry("deviceTranslatedZoneKey", new ExtensionMapping("deviceTranslatedZoneKey", LongType, null)),
        entry("deviceZoneKey", new ExtensionMapping("deviceZoneKey", LongType, null)),
        entry("sTranslatedZoneKey", new ExtensionMapping("sourceTranslatedZoneKey", LongType, null)),
        entry("sZoneKey", new ExtensionMapping("sourceZoneKey", LongType, null)),
        entry("parserVersion", new ExtensionMapping("parserVersion", StringType, null)),
        entry("parserIdentifier", new ExtensionMapping("parserIdentifier", StringType, null))
    );

    private static final String INCOMPLETE_CEF_HEADER = "Incomplete CEF header";
    private static final String INVALID_CEF_FORMAT = "Invalid CEF format";
    private static final String UNESCAPED_EQUALS_SIGN = "CEF extensions contain unescaped equals sign";

    /**
     * List of allowed timestamp formats for CEF spec v27, see: Appendix A: Date Formats
     * <a href="https://www.microfocus.com/documentation/arcsight/arcsight-smartconnectors-24.2/pdfdoc/cef-implementation-standard/cef-implementation-standard.pdf">documentation</a>
     */
    private static final List<DateTimeFormatter> TIME_FORMATS = Stream.of(
        "MMM dd HH:mm:ss.SSS zzz",
        "MMM dd HH:mm:ss.SSS",
        "MMM dd HH:mm:ss zzz",
        "MMM dd HH:mm:ss",
        "MMM dd yyyy HH:mm:ss.SSS zzz",
        "MMM dd yyyy HH:mm:ss.SSS",
        "MMM dd yyyy HH:mm:ss zzz",
        "MMM dd yyyy HH:mm:ss"
    ).map(p -> DateTimeFormatter.ofPattern(p, Locale.ROOT)).toList();

    private static final List<ChronoField> CHRONO_FIELDS = List.of(
        NANO_OF_SECOND,
        SECOND_OF_DAY,
        MINUTE_OF_DAY,
        HOUR_OF_DAY,
        DAY_OF_MONTH,
        MONTH_OF_YEAR
    );

    CefEvent process(String cefString) {
        List<String> headers = parseHeaders(cefString);
        // the last 'header' is the not-yet-parsed extension string, remove and then parse it
        Map<String, String> parsedExtensions = parseExtensions(headers.removeLast());
        CefEvent event = new CefEvent();
        processHeaders(headers, event);
        processExtensions(parsedExtensions, event);
        return event;
    }

    // visible for testing
    static List<String> parseHeaders(String cefString) {
        List<String> headers = new ArrayList<>();
        int extensionStart = -1;
        final StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < cefString.length(); i++) {
            char curr = cefString.charAt(i);
            char next = i < cefString.length() - 1 ? cefString.charAt(i + 1) : '\0';
            if (curr == '\\' && next == '\\') { // an escaped backslash
                buffer.append('\\'); // emit a backslash
                i++; // and skip the next character
            } else if (curr == '\\' && next == '|') { // an escaped pipe
                buffer.append('|'); // emit a pipe
                i++; // and skip the next character
            } else if (curr == '|') { // a pipe, it's the end of a header
                headers.add(buffer.toString()); // emit the header
                buffer.setLength(0); // and reset the buffer
                if (headers.size() == 7) {
                    extensionStart = i + 1; // the extensions begin after this pipe
                    break; // we've processed all the headers, so exit the loop
                }
            } else { // any other character
                buffer.append(curr); // is just added to the header
            }
        }

        if (headers.isEmpty() || headers.getFirst().startsWith("CEF:") == false) {
            throw new IllegalArgumentException(INVALID_CEF_FORMAT);
        }

        if (headers.size() != 7) {
            throw new IllegalArgumentException(INCOMPLETE_CEF_HEADER);
        }

        // for simplicity of the interface, pack the unparsed extension string itself into the returned list of headers
        String extensionString = cefString.substring(extensionStart);
        headers.add(extensionString);

        return headers;
    }

    private static void processHeaders(List<String> headers, CefEvent event) {
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
                default -> throw new IllegalArgumentException(INVALID_CEF_FORMAT);
            }
        }
    }

    // visible for testing
    static Map<String, String> parseExtensions(String extensionString) {
        Map<String, String> extensions = new HashMap<>();
        Matcher matcher = EXTENSION_NEXT_KEY_VALUE_PATTERN.matcher(extensionString);
        int lastEnd = 0;

        List<MatchResult> allMatches = new ArrayList<>();
        while (matcher.find()) {
            allMatches.add(matcher.toMatchResult());
        }
        for (int i = 0; i < allMatches.size(); i++) {
            MatchResult match = allMatches.get(i);
            String key = match.group(1);
            String value = match.group(2);
            // Only trim the last value
            if (i == allMatches.size() - 1) {
                value = value.trim();
            }
            if (hasUnescapedEquals(value)) {
                throw new IllegalArgumentException(UNESCAPED_EQUALS_SIGN);
            }
            extensions.put(key, desanitizeExtensionVal(value));
            lastEnd = match.end();
        }
        // If there's any remaining unparsed content, throw an exception
        if (lastEnd < extensionString.length()) {
            throw new IllegalArgumentException("Invalid extensions in the CEF event: " + extensionString.substring(lastEnd));
        }
        return extensions;
    }

    private void processExtensions(Map<String, String> parsedExtensions, CefEvent event) {
        // Cleanup empty values in extensions
        if (removeEmptyValues) {
            removeEmptyValues(parsedExtensions);
        }
        // Translate extensions to possible ECS fields
        for (Map.Entry<String, String> entry : parsedExtensions.entrySet()) {
            ExtensionMapping mapping = EXTENSION_MAPPINGS.get(entry.getKey());
            if (mapping != null) {
                String ecsKey = mapping.ecsKey();
                if (ecsKey != null) {
                    // Add the ECS translation to the root of document
                    event.addRootMapping(ecsKey, convertValueToType(entry.getValue(), mapping.dataType()));
                } else {
                    // Add the extension to the CEF mappings if it doesn't have an ECS translation
                    event.addCefMapping("extensions." + mapping.key(), convertValueToType(entry.getValue(), mapping.dataType()));
                }
            } else {
                // Add the extension if the key is not in the mapping
                event.addCefMapping("extensions." + entry.getKey(), entry.getValue());
            }
        }
    }

    private static boolean hasUnescapedEquals(String value) {
        if (value == null || value.isEmpty()) {
            return false; // Empty or null strings have no unescaped equals signs
        }

        // If there are no equals signs at all, return false
        if (value.indexOf('=') < 0) {
            return false;
        }

        boolean escaped = true;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (escaped == false) {
                escaped = true; // Reset escape flag after processing an escaped character
            } else if (c == '\\') {
                escaped = false; // Set escape flag when a backslash is encountered
            } else if (c == '=') {
                return true; // Found an unescaped equals sign, so return immediately
            }
        }
        // If we get here without finding an unescaped equals sign, return false
        return false;
    }

    private Object convertValueToType(String value, DataType type) {
        return switch (type) {
            case StringType -> value;
            case IntegerType -> Integer.parseInt(value);
            case LongType -> Long.parseLong(value);
            case DoubleType -> Double.parseDouble(value);
            case TimestampType -> toTimestamp(value);
            case MACAddressType -> toMACAddress(value);
            case IPType -> toIP(value);
        };
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
        for (DateTimeFormatter formatter : TIME_FORMATS) {
            try {
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

    // visible for testing
    String toMACAddress(String v) throws IllegalArgumentException {
        // Insert separators if necessary
        String macWithSeparators = insertMACSeparators(v);
        // Validate MAC address format
        Matcher matcher = MAC_ADDRESS_PATTERN.matcher(macWithSeparators);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException("Invalid MAC address format");
        }
        return macWithSeparators;
    }

    // visible for testing
    String toIP(String v) {
        try {
            return NetworkAddress.format(InetAddresses.forString(v));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid IP address format", e);
        }
    }

    private static String insertMACSeparators(String v) {
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

    private static void removeEmptyValues(Map<String, String> map) {
        map.values().removeIf(Strings::isEmpty);
    }

    private static String desanitizeExtensionVal(String value) {
        String desanitized = value;
        for (Map.Entry<String, String> entry : EXTENSION_VALUE_SANITIZER_REVERSE_MAPPING.entrySet()) {
            desanitized = desanitized.replace(entry.getKey(), entry.getValue());
        }
        return desanitized;
    }

    static class CefEvent implements AutoCloseable {
        private Map<String, Object> rootMappings = new HashMap<>();
        private Map<String, Object> cefMappings = new HashMap<>();

        void addRootMapping(String key, Object value) {
            this.rootMappings.put(key, value);
        }

        void addCefMapping(String key, Object value) {
            this.cefMappings.put(key, value);
        }

        Map<String, Object> getRootMappings() {
            return Objects.requireNonNull(rootMappings);
        }

        Map<String, Object> getCefMappings() {
            return Objects.requireNonNull(cefMappings);
        }

        /**
         * Nulls out the maps of the event so that future calls to methods of this class will fail with a
         * {@link NullPointerException}.
         */
        @Override
        public void close() {
            this.rootMappings = null;
            this.cefMappings = null;
        }
    }

    private record ExtensionMapping(String key, DataType dataType, @Nullable String ecsKey) {
        ExtensionMapping {
            Objects.requireNonNull(key);
            Objects.requireNonNull(dataType);
        }
    }
}
