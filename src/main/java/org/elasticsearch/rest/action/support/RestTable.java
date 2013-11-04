/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class RestTable {

    public static RestResponse buildResponse(Table table, RestRequest request, RestChannel channel) throws Exception {
        XContentType xContentType = XContentType.fromRestContentType(request.param("format", request.header("Content-Type")));
        if (xContentType != null) {
            return buildXContentBuilder(table, request, channel);
        }
        return buildTextPlainResponse(table, request, channel);
    }

    public static RestResponse buildXContentBuilder(Table table, RestRequest request, RestChannel channel) throws Exception {
        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
        Set<String> displayHeaders = buildDisplayHeaders(table, request);

        List<Table.Cell> headers = table.getHeaders();
        builder.startArray();
        for (List<Table.Cell> row : table.getRows()) {
            builder.startObject();
            for (int i = 0; i < headers.size(); i++) {
                String headerName = headers.get(i).value.toString();
                if (displayHeaders.contains(headerName)) {
                    builder.field(headerName, renderValue(request, row.get(i).value));
                }
            }
            builder.endObject();
        }
        builder.endArray();
        return new XContentRestResponse(request, RestStatus.OK, builder);
    }

    public static RestResponse buildTextPlainResponse(Table table, RestRequest request, RestChannel channel) {
        boolean verbose = request.paramAsBoolean("v", false);
        int[] width = buildWidths(table, request, verbose);
        Set<String> displayHeaders = buildDisplayHeaders(table, request);

        StringBuilder out = new StringBuilder();
        if (verbose) {
            // print the headers
            for (int i = 0; i < width.length; i++) {
                String headerName = table.getHeaders().get(i).value.toString();
                if (displayHeaders.contains(headerName)) {
                    pad(table.getHeaders().get(i), width[i], request, out);
                    out.append(" ");
                }
            }
            out.append("\n");
        }
        for (List<Table.Cell> row : table.getRows()) {
            for (int i = 0; i < width.length; i++) {
                String headerName = table.getHeaders().get(i).value.toString();
                if (displayHeaders.contains(headerName)) {
                    pad(row.get(i), width[i], request, out);
                    out.append(" ");
                }
            }
            out.append("\n");
        }

        return new StringRestResponse(RestStatus.OK, out.toString());
    }

    private static Set<String> buildDisplayHeaders(Table table, RestRequest request) {
        String pHeaders = request.param("headers");
        Set<String> display;
        if (pHeaders != null) {
            display = Strings.commaDelimitedListToSet(pHeaders);
        } else {
            display = new HashSet<String>();
            for (Table.Cell cell : table.getHeaders()) {
                String d = cell.attr.get("default");
                if (Booleans.parseBoolean(d, true)) {
                    display.add(cell.value.toString());
                }
            }
        }
        return display;
    }

    private static int[] buildWidths(Table table, RestRequest request, boolean verbose) {
        int[] width = new int[table.getHeaders().size()];

        if (verbose) {
            for (int col = 0; col < width.length; col++) {
                String v = renderValue(request, table.getHeaders().get(col).value);
                int vWidth = v == null ? 0 : v.length();
                if (width[col] < vWidth) {
                    width[col] = vWidth;
                }
            }
        }

        for (List<Table.Cell> row : table.getRows()) {
            for (int col = 0; col < width.length; col++) {
                String v = renderValue(request, row.get(col).value);
                int vWidth = v == null ? 0 : v.length();
                if (width[col] < vWidth) {
                    width[col] = vWidth;
                }
            }
        }
        return width;
    }

    private static void pad(Table.Cell cell, int width, RestRequest request, StringBuilder out) {
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
}
