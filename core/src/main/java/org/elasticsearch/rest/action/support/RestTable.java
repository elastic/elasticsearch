/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.support;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.io.UTF8StreamWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.*;

/**
 */
public class RestTable {

    public static RestResponse buildResponse(Table table, RestChannel channel) throws Exception {
        RestRequest request = channel.request();
        XContentType xContentType = XContentType.fromRestContentType(request.param("format", request.header("Content-Type")));
        if (xContentType != null) {
            return buildXContentBuilder(table, channel);
        }
        return buildTextPlainResponse(table, channel);
    }

    public static RestResponse buildXContentBuilder(Table table, RestChannel channel) throws Exception {
        RestRequest request = channel.request();
        XContentBuilder builder = channel.newBuilder();
        List<DisplayHeader> displayHeaders = buildDisplayHeaders(table, request);

        builder.startArray();
        for (int row = 0; row < table.getRows().size(); row++) {
            builder.startObject();
            for (DisplayHeader header : displayHeaders) {
                builder.field(header.display, renderValue(request, table.getAsMap().get(header.name).get(row).value));
            }
            builder.endObject();

        }
        builder.endArray();
        return new BytesRestResponse(RestStatus.OK, builder);
    }

    public static RestResponse buildTextPlainResponse(Table table, RestChannel channel) throws IOException {
        RestRequest request = channel.request();
        boolean verbose = request.paramAsBoolean("v", false);

        List<DisplayHeader> headers = buildDisplayHeaders(table, request);
        int[] width = buildWidths(table, request, verbose, headers);

        BytesStreamOutput bytesOut = channel.bytesOutput();
        UTF8StreamWriter out = new UTF8StreamWriter().setOutput(bytesOut);
        if (verbose) {
            for (int col = 0; col < headers.size(); col++) {
                DisplayHeader header = headers.get(col);
                pad(new Table.Cell(header.display, table.findHeaderByName(header.name)), width[col], request, out);
                out.append(" ");
            }
            out.append("\n");
        }

        for (int row = 0; row < table.getRows().size(); row++) {
            for (int col = 0; col < headers.size(); col++) {
                DisplayHeader header = headers.get(col);
                pad(table.getAsMap().get(header.name).get(row), width[col], request, out);
                out.append(" ");
            }
            out.append("\n");
        }
        out.close();
        return new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, bytesOut.bytes());
    }

    static List<DisplayHeader> buildDisplayHeaders(Table table, RestRequest request) {
        List<DisplayHeader> display = new ArrayList<>();
        if (request.hasParam("h")) {
            Set<String> headers = expandHeadersFromRequest(table, request);

            for (String possibility : headers) {
                DisplayHeader dispHeader = null;

                if (table.getAsMap().containsKey(possibility)) {
                    dispHeader = new DisplayHeader(possibility, possibility);
                } else {
                    for (Table.Cell headerCell : table.getHeaders()) {
                        String aliases = headerCell.attr.get("alias");
                        if (aliases != null) {
                            for (String alias : Strings.splitStringByCommaToArray(aliases)) {
                                if (possibility.equals(alias)) {
                                    dispHeader = new DisplayHeader(headerCell.value.toString(), alias);
                                    break;
                                }
                            }
                        }
                    }
                }

                if (dispHeader != null) {
                    // We know we need the header asked for:
                    display.add(dispHeader);

                    // Look for accompanying sibling column
                    Table.Cell hcell = table.getHeaderMap().get(dispHeader.name);
                    String siblingFlag = hcell.attr.get("sibling");
                    if (siblingFlag != null) {
                        // ...link the sibling and check that its flag is set
                        String sibling = siblingFlag + "." + dispHeader.name;
                        Table.Cell c = table.getHeaderMap().get(sibling);
                        if (c != null && request.paramAsBoolean(siblingFlag, false)) {
                            display.add(new DisplayHeader(c.value.toString(), siblingFlag + "." + dispHeader.display));
                        }
                    }
                }
            }
        } else {
            for (Table.Cell cell : table.getHeaders()) {
                String d = cell.attr.get("default");
                if (Booleans.parseBoolean(d, true)) {
                    display.add(new DisplayHeader(cell.value.toString(), cell.value.toString()));
                }
            }
        }
        return display;
    }

    /**
     * Extracts all the required fields from the RestRequest 'h' parameter. In order to support wildcards like
     * 'bulk.*' this needs potentially parse all the configured headers and its aliases and needs to ensure
     * that everything is only added once to the returned headers, even if 'h=bulk.*.bulk.*' is specified
     * or some headers are contained twice due to matching aliases
     */
    private static Set<String> expandHeadersFromRequest(Table table, RestRequest request) {
        Set<String> headers = new LinkedHashSet<>(table.getHeaders().size());

        // check headers and aliases
        for (String header : Strings.splitStringByCommaToArray(request.param("h"))) {
            if (Regex.isSimpleMatchPattern(header)) {
                for (Table.Cell tableHeaderCell : table.getHeaders()) {
                    String configuredHeader = tableHeaderCell.value.toString();
                    if (Regex.simpleMatch(header, configuredHeader)) {
                        headers.add(configuredHeader);
                    } else if (tableHeaderCell.attr.containsKey("alias")) {
                        String[] aliases = Strings.splitStringByCommaToArray(tableHeaderCell.attr.get("alias"));
                        for (String alias : aliases) {
                            if (Regex.simpleMatch(header, alias)) {
                                headers.add(configuredHeader);
                                break;
                            }
                        }
                    }
                }
            } else {
                headers.add(header);
            }
        }

        return headers;
    }

    public static int[] buildHelpWidths(Table table, RestRequest request) {
        int[] width = new int[3];
        for (Table.Cell cell : table.getHeaders()) {
            String v = renderValue(request, cell.value);
            int vWidth = v == null ? 0 : v.length();
            if (width[0] < vWidth) {
                width[0] = vWidth;
            }

            v = renderValue(request, cell.attr.containsKey("alias") ? cell.attr.get("alias") : "");
            vWidth = v == null ? 0 : v.length();
            if (width[1] < vWidth) {
                width[1] = vWidth;
            }

            v = renderValue(request, cell.attr.containsKey("desc") ? cell.attr.get("desc") : "not available");
            vWidth = v == null ? 0 : v.length();
            if (width[2] < vWidth) {
                width[2] = vWidth;
            }
        }
        return width;
    }

    private static int[] buildWidths(Table table, RestRequest request, boolean verbose, List<DisplayHeader> headers) {
        int[] width = new int[headers.size()];
        int i;

        if (verbose) {
            i = 0;
            for (DisplayHeader hdr : headers) {
                int vWidth = hdr.display.length();
                if (width[i] < vWidth) {
                    width[i] = vWidth;
                }
                i++;
            }
        }

        i = 0;
        for (DisplayHeader hdr : headers) {
            for (Table.Cell cell : table.getAsMap().get(hdr.name)) {
                String v = renderValue(request, cell.value);
                int vWidth = v == null ? 0 : v.length();
                if (width[i] < vWidth) {
                    width[i] = vWidth;
                }
            }
            i++;
        }
        return width;
    }

    public static void pad(Table.Cell cell, int width, RestRequest request, UTF8StreamWriter out) throws IOException {
        String sValue = renderValue(request, cell.value);
        int length = sValue == null ? 0 : sValue.length();
        byte leftOver = (byte) (width - length);
        String textAlign = cell.attr.get("text-align");
        if (textAlign == null) {
            textAlign = "left";
        }
        if (leftOver > 0 && textAlign.equals("right")) {
            for (byte i = 0; i < leftOver; i++) {
                out.append(" ");
            }
            if (sValue != null) {
                out.append(sValue);
            }
        } else {
            if (sValue != null) {
                out.append(sValue);
            }
            for (byte i = 0; i < leftOver; i++) {
                out.append(" ");
            }
        }
    }

    private static String renderValue(RestRequest request, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ByteSizeValue) {
            ByteSizeValue v = (ByteSizeValue) value;
            String resolution = request.param("bytes");
            if ("b".equals(resolution)) {
                return Long.toString(v.bytes());
            } else if ("k".equals(resolution)) {
                return Long.toString(v.kb());
            } else if ("m".equals(resolution)) {
                return Long.toString(v.mb());
            } else if ("g".equals(resolution)) {
                return Long.toString(v.gb());
            } else if ("t".equals(resolution)) {
                return Long.toString(v.tb());
            } else if ("p".equals(resolution)) {
                return Long.toString(v.pb());
            } else {
                return v.toString();
            }
        }
        if (value instanceof SizeValue) {
            SizeValue v = (SizeValue) value;
            String resolution = request.param("size");
            if ("b".equals(resolution)) {
                return Long.toString(v.singles());
            } else if ("k".equals(resolution)) {
                return Long.toString(v.kilo());
            } else if ("m".equals(resolution)) {
                return Long.toString(v.mega());
            } else if ("g".equals(resolution)) {
                return Long.toString(v.giga());
            } else if ("t".equals(resolution)) {
                return Long.toString(v.tera());
            } else if ("p".equals(resolution)) {
                return Long.toString(v.peta());
            } else {
                return v.toString();
            }
        }
        if (value instanceof TimeValue) {
            TimeValue v = (TimeValue) value;
            String resolution = request.param("time");
            if ("ms".equals(resolution)) {
                return Long.toString(v.millis());
            } else if ("s".equals(resolution)) {
                return Long.toString(v.seconds());
            } else if ("m".equals(resolution)) {
                return Long.toString(v.minutes());
            } else if ("h".equals(resolution)) {
                return Long.toString(v.hours());
            } else {
                return v.toString();
            }
        }
        // Add additional built in data points we can render based on request parameters?
        return value.toString();
    }

    static class DisplayHeader {
        public final String name;
        public final String display;

        DisplayHeader(String name, String display) {
            this.name = name;
            this.display = display;
        }
    }
}
