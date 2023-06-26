/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class RestTable {

    public static RestResponse buildResponse(Table table, RestChannel channel) throws Exception {
        RestRequest request = channel.request();
        XContentType xContentType = getResponseContentType(request);
        if (xContentType != null) {
            return buildXContentBuilder(table, channel);
        }
        return buildTextPlainResponse(table, channel);
    }

    private static XContentType getResponseContentType(RestRequest request) {
        if (request.hasParam("format")) {
            return XContentType.fromFormat(request.param("format"));
        }
        if (request.getParsedAccept() != null) {
            return request.getParsedAccept().toMediaType(XContentType.MEDIA_TYPE_REGISTRY);
        }
        return null;
    }

    public static RestResponse buildXContentBuilder(Table table, RestChannel channel) throws Exception {
        final RestRequest request = channel.request();
        final List<Integer> rowOrder = getRowOrder(table, channel.request());
        final List<DisplayHeader> displayHeaders = buildDisplayHeaders(table, request);

        return new RestResponse(
            RestStatus.OK,
            ChunkedRestResponseBody.fromXContent(
                ignored -> Iterators.concat(
                    Iterators.single((builder, params) -> builder.startArray()),
                    rowOrder.stream().<ToXContent>map(row -> (builder, params) -> {
                        builder.startObject();
                        for (DisplayHeader header : displayHeaders) {
                            builder.field(header.display, renderValue(request, table.getAsMap().get(header.name).get(row).value));
                        }
                        builder.endObject();
                        return builder;
                    }).iterator(),
                    Iterators.single((builder, params) -> builder.endArray())
                ),
                ToXContent.EMPTY_PARAMS,
                channel
            )
        );
    }

    public static RestResponse buildTextPlainResponse(Table table, RestChannel channel) {
        RestRequest request = channel.request();
        boolean verbose = request.paramAsBoolean("v", false);

        List<DisplayHeader> headers = buildDisplayHeaders(table, request);
        int[] width = buildWidths(table, request, verbose, headers);
        int lastHeader = headers.size() - 1;
        List<Integer> rowOrder = getRowOrder(table, request);

        if (verbose == false && rowOrder.isEmpty()) {
            return new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
        }

        return new RestResponse(RestStatus.OK, new ChunkedRestResponseBody() {

            private boolean needsHeader = verbose;
            private final Iterator<Integer> rowIterator = rowOrder.iterator();

            private RecyclerBytesStreamOutput currentOutput;
            private final Writer writer = new OutputStreamWriter(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    assert currentOutput != null;
                    currentOutput.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    assert currentOutput != null;
                    currentOutput.write(b, off, len);
                }

                @Override
                public void flush() {
                    assert currentOutput != null;
                    currentOutput.flush();
                }

                @Override
                public void close() {
                    assert currentOutput != null;
                    currentOutput.flush();
                }
            }, StandardCharsets.UTF_8);

            @Override
            public boolean isDone() {
                return needsHeader == false && rowIterator.hasNext() == false;
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                try {
                    assert currentOutput == null;
                    currentOutput = new RecyclerBytesStreamOutput(recycler);

                    if (needsHeader) {
                        needsHeader = false;
                        for (int col = 0; col < headers.size(); col++) {
                            DisplayHeader header = headers.get(col);
                            boolean isLastColumn = col == lastHeader;
                            pad(
                                new Table.Cell(header.display, table.findHeaderByName(header.name)),
                                width[col],
                                request,
                                writer,
                                isLastColumn
                            );
                            if (isLastColumn == false) {
                                writer.append(" ");
                            }
                        }
                        writer.append("\n");
                    }

                    while (rowIterator.hasNext() && currentOutput.size() < sizeHint) {
                        final var row = rowIterator.next();
                        for (int col = 0; col < headers.size(); col++) {
                            DisplayHeader header = headers.get(col);
                            boolean isLastColumn = col == lastHeader;
                            pad(table.getAsMap().get(header.name).get(row), width[col], request, writer, isLastColumn);
                            if (isLastColumn == false) {
                                writer.append(" ");
                            }
                        }
                        writer.append("\n");
                    }

                    if (rowIterator.hasNext()) {
                        writer.flush();
                    } else {
                        writer.close();
                    }

                    final var chunkOutput = currentOutput;
                    final var result = new ReleasableBytesReference(
                        chunkOutput.bytes(),
                        () -> Releasables.closeExpectNoException(chunkOutput)
                    );
                    currentOutput = null;
                    return result;
                } finally {
                    if (currentOutput != null) {
                        assert false : "failure encoding table chunk";
                        Releasables.closeExpectNoException(currentOutput);
                        currentOutput = null;
                    }
                }
            }

            @Override
            public String getResponseContentTypeString() {
                return RestResponse.TEXT_CONTENT_TYPE;
            }
        });
    }

    static List<Integer> getRowOrder(Table table, RestRequest request) {
        String[] columnOrdering = request.paramAsStringArray("s", null);

        List<Integer> rowOrder = new ArrayList<>();
        for (int i = 0; i < table.getRows().size(); i++) {
            rowOrder.add(i);
        }

        if (columnOrdering != null) {
            Map<String, String> headerAliasMap = table.getAliasMap();
            List<ColumnOrderElement> ordering = new ArrayList<>();
            for (int i = 0; i < columnOrdering.length; i++) {
                String columnHeader = columnOrdering[i];
                boolean reverse = false;
                if (columnHeader.endsWith(":desc")) {
                    columnHeader = columnHeader.substring(0, columnHeader.length() - ":desc".length());
                    reverse = true;
                } else if (columnHeader.endsWith(":asc")) {
                    columnHeader = columnHeader.substring(0, columnHeader.length() - ":asc".length());
                }
                if (headerAliasMap.containsKey(columnHeader)) {
                    ordering.add(new ColumnOrderElement(headerAliasMap.get(columnHeader), reverse));
                } else {
                    throw new UnsupportedOperationException(
                        String.format(Locale.ROOT, "Unable to sort by unknown sort key `%s`", columnHeader)
                    );
                }
            }
            Collections.sort(rowOrder, new TableIndexComparator(table, ordering));
        }
        return rowOrder;
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

                if (dispHeader != null && checkOutputTimestamp(dispHeader, request)) {
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
                if (Booleans.parseBoolean(d, true) && checkOutputTimestamp(cell.value.toString(), request)) {
                    display.add(new DisplayHeader(cell.value.toString(), cell.value.toString()));
                }
            }
        }
        return display;
    }

    static boolean checkOutputTimestamp(DisplayHeader dispHeader, RestRequest request) {
        return checkOutputTimestamp(dispHeader.name, request);
    }

    static boolean checkOutputTimestamp(String disp, RestRequest request) {
        if (Table.TIMESTAMP.equals(disp) || Table.EPOCH.equals(disp)) {
            return request.paramAsBoolean("ts", true);
        } else {
            return true;
        }
    }

    /**
     * Extracts all the required fields from the RestRequest 'h' parameter. In order to support wildcards like
     * 'bulk.*' this needs potentially parse all the configured headers and its aliases and needs to ensure
     * that everything is only added once to the returned headers, even if 'h=bulk.*.bulk.*' is specified
     * or some headers are contained twice due to matching aliases
     */
    private static Set<String> expandHeadersFromRequest(Table table, RestRequest request) {
        Set<String> headers = Sets.newLinkedHashSetWithExpectedSize(table.getHeaders().size());

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

    public static void pad(Table.Cell cell, int width, RestRequest request, Writer out) throws IOException {
        pad(cell, width, request, out, false);
    }

    public static void pad(Table.Cell cell, int width, RestRequest request, Writer out, boolean isLast) throws IOException {
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
            // Ignores the leftover spaces if the cell is the last of the column.
            if (isLast == false) {
                for (byte i = 0; i < leftOver; i++) {
                    out.append(" ");
                }
            }
        }
    }

    private static String renderValue(RestRequest request, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ByteSizeValue v) {
            String resolution = request.param("bytes");
            if ("b".equals(resolution)) {
                return Long.toString(v.getBytes());
            } else if ("k".equals(resolution) || "kb".equals(resolution)) {
                return Long.toString(v.getKb());
            } else if ("m".equals(resolution) || "mb".equals(resolution)) {
                return Long.toString(v.getMb());
            } else if ("g".equals(resolution) || "gb".equals(resolution)) {
                return Long.toString(v.getGb());
            } else if ("t".equals(resolution) || "tb".equals(resolution)) {
                return Long.toString(v.getTb());
            } else if ("p".equals(resolution) || "pb".equals(resolution)) {
                return Long.toString(v.getPb());
            } else {
                return v.toString();
            }
        }
        if (value instanceof SizeValue v) {
            String resolution = request.param("size");
            if ("".equals(resolution)) {
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
        if (value instanceof TimeValue v) {
            String resolution = request.param("time");
            if ("nanos".equals(resolution)) {
                return Long.toString(v.nanos());
            } else if ("micros".equals(resolution)) {
                return Long.toString(v.micros());
            } else if ("ms".equals(resolution)) {
                return Long.toString(v.millis());
            } else if ("s".equals(resolution)) {
                return Long.toString(v.seconds());
            } else if ("m".equals(resolution)) {
                return Long.toString(v.minutes());
            } else if ("h".equals(resolution)) {
                return Long.toString(v.hours());
            } else if ("d".equals(resolution)) {
                return Long.toString(v.days());
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

    static class TableIndexComparator implements Comparator<Integer> {
        private final Table table;
        private final int maxIndex;
        private final List<ColumnOrderElement> ordering;

        TableIndexComparator(Table table, List<ColumnOrderElement> ordering) {
            this.table = table;
            this.maxIndex = table.getRows().size();
            this.ordering = ordering;
        }

        @SuppressWarnings("unchecked")
        private static int compareCell(Object o1, Object o2) {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null) {
                return -1;
            } else if (o2 == null) {
                return 1;
            } else {
                if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass())) {
                    return ((Comparable) o1).compareTo(o2);
                } else {
                    return o1.toString().compareTo(o2.toString());
                }
            }
        }

        @Override
        public int compare(Integer rowIndex1, Integer rowIndex2) {
            if (rowIndex1 < maxIndex && rowIndex1 >= 0 && rowIndex2 < maxIndex && rowIndex2 >= 0) {
                Map<String, List<Table.Cell>> tableMap = table.getAsMap();
                for (ColumnOrderElement orderingElement : ordering) {
                    String column = orderingElement.getColumn();
                    if (tableMap.containsKey(column)) {
                        int comparison = compareCell(tableMap.get(column).get(rowIndex1).value, tableMap.get(column).get(rowIndex2).value);
                        if (comparison != 0) {
                            return orderingElement.isReversed() ? -1 * comparison : comparison;
                        }
                    }
                }
                return 0;
            } else {
                throw new AssertionError(
                    String.format(
                        Locale.ENGLISH,
                        "Invalid comparison of indices (%s, %s): Table has %s rows.",
                        rowIndex1,
                        rowIndex2,
                        table.getRows().size()
                    )
                );
            }
        }
    }

    static class ColumnOrderElement {
        private final String column;
        private final boolean reverse;

        ColumnOrderElement(String column, boolean reverse) {
            this.column = column;
            this.reverse = reverse;
        }

        public String getColumn() {
            return column;
        }

        public boolean isReversed() {
            return reverse;
        }
    }
}
