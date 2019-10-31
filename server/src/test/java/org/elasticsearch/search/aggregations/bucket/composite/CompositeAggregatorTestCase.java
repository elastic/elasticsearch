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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class CompositeAggregatorTestCase extends AggregatorTestCase {
    private static MappedFieldType[] FIELD_TYPES;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        FIELD_TYPES = new MappedFieldType[8];
        FIELD_TYPES[0] = new KeywordFieldMapper.KeywordFieldType();
        FIELD_TYPES[0].setName("keyword");
        FIELD_TYPES[0].setHasDocValues(true);

        FIELD_TYPES[1] = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        FIELD_TYPES[1].setName("long");
        FIELD_TYPES[1].setHasDocValues(true);

        FIELD_TYPES[2] = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        FIELD_TYPES[2].setName("double");
        FIELD_TYPES[2].setHasDocValues(true);

        DateFieldMapper.Builder builder = new DateFieldMapper.Builder("date");
        builder.docValues(true);
        builder.format("yyyy-MM-dd||epoch_millis");
        DateFieldMapper fieldMapper =
            builder.build(new Mapper.BuilderContext(createIndexSettings().getSettings(), new ContentPath(0)));
        FIELD_TYPES[3] = fieldMapper.fieldType();

        FIELD_TYPES[4] = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        FIELD_TYPES[4].setName("price");
        FIELD_TYPES[4].setHasDocValues(true);

        FIELD_TYPES[5] = new KeywordFieldMapper.KeywordFieldType();
        FIELD_TYPES[5].setName("terms");
        FIELD_TYPES[5].setHasDocValues(true);

        FIELD_TYPES[6] = new IpFieldMapper.IpFieldType();
        FIELD_TYPES[6].setName("ip");
        FIELD_TYPES[6].setHasDocValues(true);

        FIELD_TYPES[7] = new GeoPointFieldMapper.GeoPointFieldType();
        FIELD_TYPES[7].setName("geo_point");
        FIELD_TYPES[7].setHasDocValues(true);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        FIELD_TYPES = null;
    }

    protected void testSearchCase(List<Query> queries,
                                List<Map<String, List<Object>>> dataset,
                                Supplier<CompositeAggregationBuilder> create,
                                Consumer<InternalComposite> verify) throws IOException {
        for (Query query : queries) {
            executeTestCase(false, false, query, dataset, create, verify);
            executeTestCase(false, true, query, dataset, create, verify);
            executeTestCase(true, true, query, dataset, create, verify);
        }
    }

    protected void executeTestCase(boolean useIndexSort,
                                   boolean reduced,
                                   Query query,
                                   List<Map<String, List<Object>>> dataset,
                                   Supplier<CompositeAggregationBuilder> create,
                                   Consumer<InternalComposite> verify) throws IOException {
        Map<String, MappedFieldType> types =
            Arrays.stream(FIELD_TYPES).collect(Collectors.toMap(MappedFieldType::name,  Function.identity()));
        CompositeAggregationBuilder aggregationBuilder = create.get();
        Sort indexSort = useIndexSort ? buildIndexSort(aggregationBuilder.sources(), types) : null;
        IndexSettings indexSettings = createIndexSettings(indexSort);
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = newIndexWriterConfig(random(), new MockAnalyzer(random()));
            if (indexSort != null) {
                config.setIndexSort(indexSort);
            }
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config)) {
                Document document = new Document();
                for (Map<String, List<Object>> fields : dataset) {
                    addToDocument(document, fields);
                    indexWriter.addDocument(document);
                    document.clear();
                }
                if (reduced == false) {
                    indexWriter.forceMerge(1);
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = new IndexSearcher(indexReader);
                final InternalComposite composite;
                if (reduced) {
                    composite = searchAndReduce(indexSettings, indexSearcher, query, aggregationBuilder, FIELD_TYPES);
                } else {
                    composite = search(indexSettings, indexSearcher, query, aggregationBuilder, FIELD_TYPES);
                }
                verify.accept(composite);
            }
        }
    }

    private static IndexSettings createIndexSettings(Sort sort) {
        Settings.Builder builder = Settings.builder();
        if (sort != null) {
            String[] fields = Arrays.stream(sort.getSort())
                .map(SortField::getField)
                .toArray(String[]::new);
            String[] orders = Arrays.stream(sort.getSort())
                .map((o) -> o.getReverse() ? "desc" : "asc")
                .toArray(String[]::new);
            builder.putList("index.sort.field", fields);
            builder.putList("index.sort.order", orders);
        }
        return IndexSettingsModule.newIndexSettings(new Index("_index", "0"), builder.build());
    }

    private void addToDocument(Document doc, Map<String, List<Object>> keys) {
        for (Map.Entry<String, List<Object>> entry : keys.entrySet()) {
            final String name = entry.getKey();
            for (Object value : entry.getValue()) {
                if (value instanceof Integer) {
                    doc.add(new SortedNumericDocValuesField(name, (int) value));
                    doc.add(new IntPoint(name, (int) value));
                } else if (value instanceof Long) {
                    doc.add(new SortedNumericDocValuesField(name, (long) value));
                    doc.add(new LongPoint(name, (long) value));
                } else if (value instanceof Double) {
                    doc.add(new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong((double) value)));
                    doc.add(new DoublePoint(name, (double) value));
                } else if (value instanceof String) {
                    doc.add(new SortedSetDocValuesField(name, new BytesRef((String) value)));
                    doc.add(new StringField(name, new BytesRef((String) value), Field.Store.NO));
                } else if (value instanceof InetAddress) {
                    doc.add(new SortedSetDocValuesField(name, new BytesRef(InetAddressPoint.encode((InetAddress) value))));
                    doc.add(new InetAddressPoint(name, (InetAddress) value));
                } else if (value instanceof GeoPoint) {
                    GeoPoint point = (GeoPoint)value;
                    doc.add(new SortedNumericDocValuesField(name,
                        GeoTileUtils.longEncode(point.lon(), point.lat(), GeoTileGridAggregationBuilder.DEFAULT_PRECISION)));
                    doc.add(new LatLonPoint(name, point.lat(), point.lon()));
                } else {
                    throw new AssertionError("invalid object: " + value.getClass().getSimpleName());
                }
            }
        }
    }

    protected static Map<String, Object> createAfterKey(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i+=2) {
            String field = (String) fields[i];
            map.put(field, fields[i+1]);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, List<Object>> createDocument(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, List<Object>> map = new HashMap<>();
        for (int i = 0; i < fields.length; i+=2) {
            String field = (String) fields[i];
            if (fields[i+1] instanceof List) {
                map.put(field, (List<Object>) fields[i+1]);
            } else {
                map.put(field, Collections.singletonList(fields[i+1]));
            }
        }
        return map;
    }

    protected static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }

    private static Sort buildIndexSort(List<CompositeValuesSourceBuilder<?>> sources, Map<String, MappedFieldType> fieldTypes) {
        List<SortField> sortFields = new ArrayList<>();
        for (CompositeValuesSourceBuilder<?> source : sources) {
            MappedFieldType type = fieldTypes.get(source.field());
            if (type instanceof KeywordFieldMapper.KeywordFieldType) {
                sortFields.add(new SortedSetSortField(type.name(), false));
            } else if (type instanceof DateFieldMapper.DateFieldType) {
                sortFields.add(new SortedNumericSortField(type.name(), SortField.Type.LONG, false));
            } else if (type instanceof NumberFieldMapper.NumberFieldType) {
                boolean comp = false;
                switch (type.typeName()) {
                    case "byte":
                    case "short":
                    case "integer":
                        comp = true;
                        sortFields.add(new SortedNumericSortField(type.name(), SortField.Type.INT, false));
                        break;

                    case "long":
                        sortFields.add(new SortedNumericSortField(type.name(), SortField.Type.LONG, false));
                        break;

                    case "float":
                    case "double":
                        comp = true;
                        sortFields.add(new SortedNumericSortField(type.name(), SortField.Type.DOUBLE, false));
                        break;

                    default:
                        break;
                }
                if (comp == false) {
                    break;
                }
            }
        }
        return sortFields.size() > 0 ? new Sort(sortFields.toArray(new SortField[0])) : null;
    }
}
