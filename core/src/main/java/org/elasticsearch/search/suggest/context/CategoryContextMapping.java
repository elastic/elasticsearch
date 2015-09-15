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

package org.elasticsearch.search.suggest.context;

import com.google.common.base.Joiner;
import org.apache.lucene.analysis.PrefixAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;

import java.io.IOException;
import java.util.*;

/**
 * The {@link CategoryContextMapping} is used to define a {@link ContextMapping} that
 * references a field within a document. The value of the field in turn will be
 * used to setup the suggestions made by the completion suggester.
 */
public class CategoryContextMapping extends ContextMapping {

    protected static final String TYPE = "category";

    private static final String FIELD_FIELDNAME = "path";
    private static final String DEFAULT_FIELDNAME = "_type";

    private static final Iterable<? extends CharSequence> EMPTY_VALUES = Collections.emptyList();

    private final String fieldName;
    private final Iterable<? extends CharSequence> defaultValues;
    private final FieldConfig defaultConfig;
            
    /**
     * Create a new {@link CategoryContextMapping} with the default field
     * <code>[_type]</code>
     */
    public CategoryContextMapping(String name) {
        this(name, DEFAULT_FIELDNAME, EMPTY_VALUES);
    }

    /**
     * Create a new {@link CategoryContextMapping} with the default field
     * <code>[_type]</code>
     */
    public CategoryContextMapping(String name, String fieldName) {
        this(name, fieldName, EMPTY_VALUES);
    }

    /**
     * Create a new {@link CategoryContextMapping} with the default field
     * <code>[_type]</code>
     */
    public CategoryContextMapping(String name, Iterable<? extends CharSequence> defaultValues) {
        this(name, DEFAULT_FIELDNAME, defaultValues);
    }

    /**
     * Create a new {@link CategoryContextMapping} with the default field
     * <code>[_type]</code>
     */
    public CategoryContextMapping(String name, String fieldName, Iterable<? extends CharSequence> defaultValues) {
        super(TYPE, name);
        this.fieldName = fieldName;
        this.defaultValues = defaultValues;
        this.defaultConfig = new FieldConfig(fieldName, defaultValues, null);
    }

    /**
     * Name of the field used by this {@link CategoryContextMapping}
     */
    public String getFieldName() {
        return fieldName;
    }

    public Iterable<? extends CharSequence> getDefaultValues() {
        return defaultValues;
    }
    
    @Override
    public FieldConfig defaultConfig() {
        return defaultConfig;
    }

    /**
     * Load the specification of a {@link CategoryContextMapping}
     * 
     * @param field
     *            name of the field to use. If <code>null</code> default field
     *            will be used
     * @return new {@link CategoryContextMapping}
     */
    protected static CategoryContextMapping load(String name, Map<String, Object> config) throws ElasticsearchParseException {
        CategoryContextMapping.Builder mapping = new CategoryContextMapping.Builder(name);

        Object fieldName = config.get(FIELD_FIELDNAME);
        Object defaultValues = config.get(FIELD_MISSING);

        if (fieldName != null) {
            mapping.fieldName(fieldName.toString());
            config.remove(FIELD_FIELDNAME);
        }

        if (defaultValues != null) {
            if (defaultValues instanceof Iterable) {
                for (Object value : (Iterable) defaultValues) {
                    mapping.addDefaultValue(value.toString());
                }
            } else {
                mapping.addDefaultValue(defaultValues.toString());
            }
            config.remove(FIELD_MISSING);
        }

        return mapping.build();
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        if (fieldName != null) {
            builder.field(FIELD_FIELDNAME, fieldName);
        }
        builder.startArray(FIELD_MISSING);
        for (CharSequence value : defaultValues) {
            builder.value(value);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public ContextConfig parseContext(ParseContext parseContext, XContentParser parser) throws IOException, ElasticsearchParseException {
        Token token = parser.currentToken();
        if (token == Token.VALUE_NULL) {
            return new FieldConfig(fieldName, defaultValues, null);
        } else if (token == Token.VALUE_STRING) {
            return new FieldConfig(fieldName, null, Collections.singleton(parser.text()));
        } else if (token == Token.VALUE_NUMBER) {
            return new FieldConfig(fieldName, null, Collections.singleton(parser.text()));
        } else if (token == Token.VALUE_BOOLEAN) {
            return new FieldConfig(fieldName, null, Collections.singleton(parser.text()));
        } else if (token == Token.START_ARRAY) {
            ArrayList<String> values = new ArrayList<>();
            while((token = parser.nextToken()) != Token.END_ARRAY) {
                values.add(parser.text());
            }
            if(values.isEmpty()) {
                throw new ElasticsearchParseException("FieldConfig must contain a least one category");
            }
            return new FieldConfig(fieldName, null, values);
        } else {
            throw new ElasticsearchParseException("FieldConfig must be either [null], a string or a list of strings");
        }
    }

    @Override
    public FieldQuery parseQuery(String name, XContentParser parser) throws IOException, ElasticsearchParseException {
        Iterable<? extends CharSequence> values;
        Token token = parser.currentToken();
        if (token == Token.START_ARRAY) {
            ArrayList<String> list = new ArrayList<>();
            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                list.add(parser.text());
            }
            values = list;
        } else if (token == Token.VALUE_NULL) {
            values = defaultValues;
        } else {
            values = Collections.singleton(parser.text());
        }

        return new FieldQuery(name, values);
    }

    public static FieldQuery query(String name, CharSequence... fieldvalues) {
        return query(name, Arrays.asList(fieldvalues));
    }

    public static FieldQuery query(String name, Iterable<? extends CharSequence> fieldvalues) {
        return new FieldQuery(name, fieldvalues);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CategoryContextMapping) {
            CategoryContextMapping other = (CategoryContextMapping) obj;
            if (this.fieldName.equals(other.fieldName)) {
                return Iterables.allElementsAreEqual(this.defaultValues, other.defaultValues);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hashCode = fieldName.hashCode();
        for (CharSequence seq : defaultValues) {
            hashCode = 31 * hashCode + seq.hashCode();
        }
        return hashCode;
    }

    private static class FieldConfig extends ContextConfig {

        private final String fieldname;
        private final Iterable<? extends CharSequence> defaultValues;
        private final Iterable<? extends CharSequence> values;

        public FieldConfig(String fieldname, Iterable<? extends CharSequence> defaultValues, Iterable<? extends CharSequence> values) {
            this.fieldname = fieldname;
            this.defaultValues = defaultValues;
            this.values = values;
        }

        @Override
        protected TokenStream wrapTokenStream(Document doc, TokenStream stream) {
            if (values != null) {
                return new PrefixAnalyzer.PrefixTokenFilter(stream, ContextMapping.SEPARATOR, values);
            // if fieldname is default, BUT our default values are set, we take that one
            } else if ((doc.getFields(fieldname).length == 0 || fieldname.equals(DEFAULT_FIELDNAME)) && defaultValues.iterator().hasNext()) {
                return new PrefixAnalyzer.PrefixTokenFilter(stream, ContextMapping.SEPARATOR, defaultValues);
            } else {
                IndexableField[] fields = doc.getFields(fieldname);
                ArrayList<CharSequence> values = new ArrayList<>(fields.length);
                for (int i = 0; i < fields.length; i++) {
                    values.add(fields[i].stringValue());
                }

                return new PrefixAnalyzer.PrefixTokenFilter(stream, ContextMapping.SEPARATOR, values);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("FieldConfig(" + fieldname + " = [");
            if (this.values != null && this.values.iterator().hasNext()) {
                sb.append("(").append(Joiner.on(", ").join(this.values.iterator())).append(")");
            }
            if (this.defaultValues != null && this.defaultValues.iterator().hasNext()) {
                sb.append(" default(").append(Joiner.on(", ").join(this.defaultValues.iterator())).append(")");
            }
            return sb.append("])").toString();
        }

    }

    private static class FieldQuery extends ContextQuery {

        private final Iterable<? extends CharSequence> values;

        public FieldQuery(String name, Iterable<? extends CharSequence> values) {
            super(name);
            this.values = values;
        }

        @Override
        public Automaton toAutomaton() {
            List<Automaton> automatons = new ArrayList<>();
            for (CharSequence value : values) {
                automatons.add(Automata.makeString(value.toString()));
            }
            return Operations.union(automatons);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(name);
            for (CharSequence value : values) {
                builder.value(value);
            }
            builder.endArray();
            return builder;
        }
    }

    public static class Builder extends ContextBuilder<CategoryContextMapping> {

        private String fieldname;
        private List<CharSequence> defaultValues = new ArrayList<>();

        public Builder(String name) {
            this(name, DEFAULT_FIELDNAME);
        }

        public Builder(String name, String fieldname) {
            super(name);
            this.fieldname = fieldname;
        }

        /**
         * Set the name of the field to use
         */
        public Builder fieldName(String fieldname) {
            this.fieldname = fieldname;
            return this;
        }

        /**
         * Add value to the default values of the mapping
         */
        public Builder addDefaultValue(CharSequence defaultValue) {
            this.defaultValues.add(defaultValue);
            return this;
        }

        /**
         * Add set of default values to the mapping
         */
        public Builder addDefaultValues(CharSequence... defaultValues) {
            for (CharSequence defaultValue : defaultValues) {
                this.defaultValues.add(defaultValue);
            }
            return this;
        }

        /**
         * Add set of default values to the mapping
         */
        public Builder addDefaultValues(Iterable<? extends CharSequence> defaultValues) {
            for (CharSequence defaultValue : defaultValues) {
                this.defaultValues.add(defaultValue);
            }
            return this;
        }

        @Override
        public CategoryContextMapping build() {
            return new CategoryContextMapping(name, fieldname, defaultValues);
        }
    }
}
