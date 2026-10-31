/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.document.InetAddressPoint;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

/**
 * The document field named by {@link IndexSettings#ROUTING_FIELD}, whose value is the document's {@code _routing}.
 * <p>
 * The coordinating node derives {@code _routing} from the field ({@link IndexRouting}) and the data node checks that the
 * two agree when it parses the document ({@code RoutingFieldMapper}), so every document sits on the shard its field value
 * hashes to, whatever API wrote it. Both sides compute the value here so they agree byte for byte.
 * <p>
 * The routing value is the field value in a canonical string form, so that equal values produce equal routing whatever
 * way the source spells them, and the same value routes the same in two indices even when the fields have different
 * names or numeric widths:
 * <ul>
 *     <li>{@code keyword}: the text as it appears in the source, which is what a keyword without normalizer indexes;</li>
 *     <li>{@code long}, {@code integer}, {@code short}, {@code byte}: the decimal form of the integer ({@code 42} for
 *     {@code 42}, {@code "42"} and {@code 42.0}); values with a fraction are rejected rather than truncated;</li>
 *     <li>{@code ip}: the address as Elasticsearch formats {@code ip} values, so {@code ::ffff:10.0.0.1} and
 *     {@code 10.0.0.1} are the same value and IPv6 is in its compressed form.</li>
 * </ul>
 * Floating point types are not supported: equality on them depends on rounding, so they make poor routing keys.
 */
public final class RoutingField {

    /**
     * Field types that can be routing fields, grouped by how their values are made canonical.
     */
    public enum Type {
        KEYWORD,
        INTEGRAL,
        IP;

        /**
         * The routing field type for a mapped field type name, or {@code null} if that type cannot be a routing field.
         */
        @Nullable
        public static Type forFieldType(String typeName) {
            return switch (typeName) {
                case "keyword" -> KEYWORD;
                case "long", "integer", "short", "byte" -> INTEGRAL;
                case "ip" -> IP;
                default -> null;
            };
        }
    }

    /**
     * Mapped field types allowed as routing fields, for error messages.
     */
    public static final Set<String> SUPPORTED_FIELD_TYPES = Set.of("keyword", "long", "integer", "short", "byte", "ip");

    private final String path;
    private final Type type;
    private final XContentParserConfiguration parserConfig;

    public RoutingField(String path, Type type) {
        this.path = path;
        this.type = type;
        this.parserConfig = XContentParserConfiguration.EMPTY.withFiltering(null, Set.of(path), null, true);
    }

    public String path() {
        return path;
    }

    /**
     * A parser configuration that only reads the routing field, for {@link #extract(XContentParser)}.
     */
    public XContentParserConfiguration parserConfig() {
        return parserConfig;
    }

    /**
     * The canonical routing value of a document's source.
     */
    public String extract(XContentType contentType, BytesReference source) {
        try (XContentParser parser = XContentHelper.createParserNotCompressed(parserConfig, source, contentType)) {
            return extract(parser);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error extracting routing field [" + path + "]: " + e.getMessage(), e);
        }
    }

    /**
     * The canonical routing value read from a parser created with {@link #parserConfig()}.
     *
     * @throws IllegalArgumentException if the document has no value, more than one value, or a value of the wrong kind
     */
    public String extract(XContentParser parser) throws IOException {
        String value = null;
        int count = 0;
        for (XContentParser.Token token = parser.nextToken(); token != null; token = parser.nextToken()) {
            if (token.isValue()) {
                count++;
                value = canonical(token, parser);
            }
        }
        if (count == 0) {
            throw new IllegalArgumentException("routing field [" + path + "] is missing or null; it is required to route the document");
        }
        if (count > 1) {
            throw new IllegalArgumentException("routing field [" + path + "] must have a single value, found [" + count + "]");
        }
        return value;
    }

    private String canonical(XContentParser.Token token, XContentParser parser) throws IOException {
        return switch (type) {
            case KEYWORD -> parser.text();
            case INTEGRAL -> canonicalIntegral(token, parser);
            case IP -> canonicalIp(token, parser);
        };
    }

    private String canonicalIntegral(XContentParser.Token token, XContentParser parser) throws IOException {
        String text = parser.text();
        if (token == XContentParser.Token.VALUE_NUMBER || token == XContentParser.Token.VALUE_STRING) {
            try {
                return canonicalIntegral(new BigDecimal(text.trim()).longValueExact());
            } catch (ArithmeticException | NumberFormatException e) {
                // fall through to the error below
            }
        }
        throw new IllegalArgumentException("routing field [" + path + "] must be an integer, found [" + text + "]");
    }

    private String canonicalIp(XContentParser.Token token, XContentParser parser) throws IOException {
        String text = parser.text();
        if (token == XContentParser.Token.VALUE_STRING) {
            try {
                return canonicalIp(InetAddressPoint.encode(InetAddresses.forString(text)));
            } catch (IllegalArgumentException e) {
                // fall through to the error below
            }
        }
        throw new IllegalArgumentException("routing field [" + path + "] must be an IP address, found [" + text + "]");
    }

    /**
     * The routing value of an integral field value. Readers of indexed or computed values (such as the ES|QL co-located join,
     * which routes join keys to the shard that holds their matches) use this so that they agree with the indexed documents.
     */
    public static String canonicalIntegral(long value) {
        return Long.toString(value);
    }

    /**
     * The routing value of an {@code ip} field value in its indexed form: the 16 bytes of
     * {@link InetAddressPoint#encode}. See {@link #canonicalIntegral(long)}.
     */
    public static String canonicalIp(byte[] encoded) {
        // Decoding the indexed form turns IPv4-mapped IPv6 addresses back into IPv4, as for ip fields.
        return NetworkAddress.format(InetAddressPoint.decode(encoded));
    }

    /**
     * The error for a document whose {@code _routing} is not the value of its routing field.
     */
    public String mismatch(String routing, String value) {
        return "routing ["
            + routing
            + "] does not match the value ["
            + value
            + "] of routing field ["
            + path
            + "]; the routing field cannot change in an update, delete and re-index the document instead";
    }

    /**
     * The type of the routing field as declared in an index mapping, or {@code null} if it is not mapped (yet) or has a type
     * that cannot route. The coordinating node has no mapping lookup, only the mapping source.
     */
    @Nullable
    public static Type typeFromMapping(@Nullable MappingMetadata mapping, String path) {
        if (mapping == null) {
            return null;
        }
        Object typeName = findFieldType(mapping.sourceAsMap(), path);
        return typeName instanceof String name ? Type.forFieldType(name) : null;
    }

    /**
     * Finds the {@code type} of a field in a mapping source, following {@code properties} and allowing any part of the path
     * to be written with dots (as with {@code subobjects: false}).
     */
    @Nullable
    private static Object findFieldType(Map<?, ?> mapping, String path) {
        if (mapping.get("properties") instanceof Map<?, ?> properties) {
            if (properties.get(path) instanceof Map<?, ?> field) {
                return field.get("type");
            }
            for (int dot = path.indexOf('.'); dot >= 0; dot = path.indexOf('.', dot + 1)) {
                if (properties.get(path.substring(0, dot)) instanceof Map<?, ?> object) {
                    Object type = findFieldType(object, path.substring(dot + 1));
                    if (type != null) {
                        return type;
                    }
                }
            }
        }
        return null;
    }
}
