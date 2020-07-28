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

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

public class SearchLookup {
    /**
     * The maximum "depth" of field dependencies. When a field's doc values
     * depends on another field's doc values and depends on another field's
     * doc values that depends on another field's doc values, etc, etc,
     * it can make a very deep stack. At some point we're concerned about
     * {@link StackOverflowError}. This limits that "depth".
     */
    static final int MAX_FIELD_CHAIN = 30;

    /**
     * The chain of fields for which this lookup was created used for detecting
     * loops in doc values calculations. It is empty for the "top level" lookup
     * created for the entire search. When a lookup is created for a field we
     * make sure its name isn't in the list then add it to the end. So the
     * lookup for the a field named {@code foo} will be {@code ["foo"]}. If
     * that field looks up the values of a field named {@code bar} then
     * {@code bar}'s lookup will contain {@code ["foo", "bar"]}.
     */
    private final List<String> fieldChain;
    private final MapperService mapperService;
    private final BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> indexFieldDataService;
    private final DocLookup docMap;
    private final SourceLookup sourceLookup;
    private final FieldsLookup fieldsLookup;

    /**
     * Create the "top level" field lookup for a search request.
     */
    public SearchLookup(
        MapperService mapperService,
        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> indexFieldDataService
    ) {
        this(emptyList(), mapperService, indexFieldDataService, new SourceLookup(), new FieldsLookup(mapperService));
    }

    private SearchLookup(
        List<String> fieldChain,
        MapperService mapperService,
        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> indexFieldDataService,
        SourceLookup sourceLookup,
        FieldsLookup fieldsLookup
    ) {
        this.fieldChain = fieldChain;
        this.mapperService = mapperService;;
        this.indexFieldDataService = indexFieldDataService;
        this.docMap = new DocLookup(mapperService, ft -> this.indexFieldDataService.apply(ft, () -> forField(ft.name())));
        this.sourceLookup = sourceLookup;
        this.fieldsLookup = fieldsLookup;
    }

    /**
     * Build a new lookup for a field that needs the lookup to create doc values.
     *
     * @throws IllegalArgumentException if it detects a loop in the fields required to build doc values
     */
    public SearchLookup forField(String name) {
        List<String> newFieldChain = new ArrayList<>(fieldChain.size() + 1);
        newFieldChain.addAll(fieldChain);
        newFieldChain.add(name);
        if (fieldChain.contains(name)) {
            throw new IllegalArgumentException("loop in field definitions: " + newFieldChain);
        }
        if (newFieldChain.size() > MAX_FIELD_CHAIN) {
            throw new IllegalArgumentException("field definition too deep: " + newFieldChain);
        }
        return new SearchLookup(unmodifiableList(newFieldChain), mapperService, indexFieldDataService, sourceLookup, fieldsLookup);
    }

    /**
     * Builds a new {@linkplain LeafSearchLookup} for the caller to handle
     * a {@linkplain LeafReaderContext}. This lookup shares the {@code _source}
     * lookup but doesn't share doc values or stored fields.
     */
    public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
        return new LeafSearchLookup(context,
                docMap.getLeafDocLookup(context),
                sourceLookup,
                fieldsLookup.getLeafFieldsLookup(context));
    }

    public DocLookup doc() {
        return docMap;
    }

    public SourceLookup source() {
        return sourceLookup;
    }
}
