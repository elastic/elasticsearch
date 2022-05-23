/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.document;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.LookupField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;

/**
 * A single field name and values part of {@link SearchHit} and {@link GetResult}.
 *
 * @see SearchHit
 * @see GetResult
 */
public class DocumentField implements Writeable, Iterable<Object> {

    private final String name;
    private final List<Object> values;
    private final List<Object> ignoredValues;
    private final List<LookupField> lookupFields;

    public DocumentField(StreamInput in) throws IOException {
        name = in.readString();
        values = in.readList(StreamInput::readGenericValue);
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            ignoredValues = in.readList(StreamInput::readGenericValue);
        } else {
            ignoredValues = Collections.emptyList();
        }
        if (in.getVersion().onOrAfter(Version.V_8_2_0)) {
            lookupFields = in.readList(LookupField::new);
        } else {
            lookupFields = List.of();
        }
    }

    public DocumentField(String name, List<Object> values) {
        this(name, values, Collections.emptyList());
    }

    public DocumentField(String name, List<Object> values, List<Object> ignoredValues) {
        this(name, values, ignoredValues, Collections.emptyList());
    }

    public DocumentField(String name, List<Object> values, List<Object> ignoredValues, List<LookupField> lookupFields) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.values = Objects.requireNonNull(values, "values must not be null");
        this.ignoredValues = Objects.requireNonNull(ignoredValues, "ignoredValues must not be null");
        this.lookupFields = Objects.requireNonNull(lookupFields, "lookupFields must not be null");
        assert lookupFields.isEmpty() || (values.isEmpty() && ignoredValues.isEmpty())
            : "DocumentField can't have both lookup fields and values";
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * The first value of the hit.
     */
    @SuppressWarnings("unchecked")
    public <V> V getValue() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return (V) values.get(0);
    }

    /**
     * The field values.
     */
    public List<Object> getValues() {
        return values;
    }

    @Override
    public Iterator<Object> iterator() {
        return values.iterator();
    }

    /**
     * The field's ignored values as an immutable list.
     */
    public List<Object> getIgnoredValues() {
        return ignoredValues == Collections.emptyList() ? ignoredValues : Collections.unmodifiableList(ignoredValues);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeCollection(values, StreamOutput::writeGenericValue);
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            out.writeCollection(ignoredValues, StreamOutput::writeGenericValue);
        }
        if (out.getVersion().onOrAfter(Version.V_8_2_0)) {
            out.writeList(lookupFields);
        } else {
            if (lookupFields.isEmpty() == false) {
                assert false : "Lookup fields require all nodes be on 8.2 or later";
                throw new IllegalStateException("Lookup fields require all nodes be on 8.2 or later");
            }
        }
    }

    public List<LookupField> getLookupFields() {
        return lookupFields;
    }

    public ToXContentFragment getValidValuesWriter() {
        return new ToXContentFragment() {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startArray(name);
                for (Object value : values) {
                    // This call doesn't really need to support writing any kind of object, since the values
                    // here are always serializable to xContent. Each value could be a leaf types like a string,
                    // number, or boolean, a list of such values, or a map of such values with string keys.
                    builder.value(value);
                }
                builder.endArray();
                return builder;
            }
        };
    }

    public ToXContentFragment getIgnoredValuesWriter() {
        return new ToXContentFragment() {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startArray(name);
                for (Object value : ignoredValues) {
                    builder.value(value);
                }
                builder.endArray();
                return builder;
            }
        };
    }

    public static DocumentField fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String fieldName = parser.currentName();
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
        List<Object> values = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            values.add(parseFieldsValue(parser));
        }
        return new DocumentField(fieldName, values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DocumentField objects = (DocumentField) o;
        return Objects.equals(name, objects.name)
            && Objects.equals(values, objects.values)
            && Objects.equals(ignoredValues, objects.ignoredValues)
            && Objects.equals(lookupFields, objects.lookupFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, values, ignoredValues, lookupFields);
    }

    @Override
    public String toString() {
        return "DocumentField{"
            + "name='"
            + name
            + '\''
            + ", values="
            + values
            + ", ignoredValues="
            + ignoredValues
            + ", lookupFields="
            + lookupFields
            + '}';
    }

}
