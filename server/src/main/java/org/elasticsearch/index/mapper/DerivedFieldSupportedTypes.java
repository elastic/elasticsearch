/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.script.field.Field;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum DerivedFieldSupportedTypes {

    BOOLEAN("boolean", (name, context, indexAnalyzers) -> {
        BooleanFieldMapper.Builder builder = new BooleanFieldMapper.Builder(name);
        return builder.build(context);
    }, name -> o -> {
        // Trying to mimic the logic for parsing source value as used in BooleanFieldMapper valueFetcher
        Boolean value;
        if (o instanceof Boolean) {
            value = (Boolean) o;
        } else {
            String textValue = o.toString();
            value = Booleans.parseBooleanStrict(textValue, false);
        }
        return new Field(name, value ? "T" : "F", BooleanFieldMapper.Defaults.FIELD_TYPE);
    }, formatter -> o -> o),
    DATE("date", (name, context, indexAnalyzers) -> {
        // TODO: should we support mapping settings exposed by a given field type from derived fields too?
        // for example, support `format` for date type?
        DateFieldMapper.Builder builder = new DateFieldMapper.Builder(
            name,
            DateFieldMapper.Resolution.MILLISECONDS,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            false,
            Version.CURRENT
        );
        return builder.build(context);
    },
        name -> o -> new LongPoint(name, (long) o),
        formatter -> o -> formatter == null
            ? DateFieldMapper.getDefaultDateTimeFormatter().formatMillis((long) o)
            : formatter.formatMillis((long) o)
    ),
    GEO_POINT("geo_point", (name, context, indexAnalyzers) -> {
        GeoPointFieldMapper.Builder builder = new GeoPointFieldMapper.Builder(name);
        return builder.build(context);
    }, name -> o -> {
        // convert o to array of double
        if (!(o instanceof Tuple) || !(((Tuple<?, ?>) o).v1() instanceof Double || !(((Tuple<?, ?>) o).v2() instanceof Double))) {
            throw new ClassCastException("geo_point should be in format emit(double lat, double lon) for derived fields");
        }
        return new LatLonPoint(name, (double) ((Tuple<?, ?>) o).v1(), (double) ((Tuple<?, ?>) o).v2());
    }, formatter -> o -> new GeoPoint((double) ((Tuple) o).v1(), (double) ((Tuple) o).v2())),
    IP("ip", (name, context, indexAnalyzers) -> {
        IpFieldMapper.Builder builder = new IpFieldMapper.Builder(name, false, Version.CURRENT);
        return builder.build(context);
    }, name -> o -> {
        InetAddress address;
        if (o instanceof InetAddress) {
            address = (InetAddress) o;
        } else {
            address = InetAddresses.forString(o.toString());
        }
        return new InetAddressPoint(name, address);
    }, formatter -> o -> o),
    KEYWORD("keyword", (name, context, indexAnalyzers) -> {
        FieldType dummyFieldType = new FieldType();
        dummyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        KeywordFieldMapper.Builder keywordBuilder = new KeywordFieldMapper.Builder(name);
        KeywordFieldMapper.KeywordFieldType keywordFieldType = keywordBuilder.buildFieldType(context, dummyFieldType);
        keywordFieldType.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
        return new KeywordFieldMapper(
            name,
            dummyFieldType,
            keywordFieldType,
            keywordBuilder.multiFieldsBuilder.build(keywordBuilder, context),
            keywordBuilder.copyTo.build(),
            keywordBuilder
        );
    }, name -> o -> new KeywordField(name, (String) o, Field.Store.NO), formatter -> o -> o),
    TEXT("text", (name, context, indexAnalyzers) -> {
        FieldType dummyFieldType = new FieldType();
        dummyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        TextFieldMapper.Builder textBuilder = new TextFieldMapper.Builder(name, indexAnalyzers);
        return textBuilder.build(context);
    }, name -> o -> new TextField(name, (String) o, Field.Store.NO), formatter -> o -> o),
    LONG("long", (name, context, indexAnalyzers) -> {
        NumberFieldMapper.Builder longBuilder = new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.LONG, false, false);
        return longBuilder.build(context);
    }, name -> o -> new LongField(name, Long.parseLong(o.toString()), Field.Store.NO), formatter -> o -> o),
    DOUBLE("double", (name, context, indexAnalyzers) -> {
        NumberFieldMapper.Builder doubleBuilder = new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.DOUBLE, false, false);
        return doubleBuilder.build(context);
    }, name -> o -> new DoubleField(name, Double.parseDouble(o.toString()), Field.Store.NO), formatter -> o -> o),
    FLOAT("float", (name, context, indexAnalyzers) -> {
        NumberFieldMapper.Builder floatBuilder = new NumberFieldMapper.Builder(name, NumberFieldMapper.NumberType.FLOAT, false, false);
        return floatBuilder.build(context);
    }, name -> o -> new FloatField(name, Float.parseFloat(o.toString()), Field.Store.NO), formatter -> o -> o),
    OBJECT("object", (name, context, indexAnalyzers) -> {
        // we create a keyword field type with index options set as NONE as we don't support queries directly on object type
        KeywordFieldMapper.Builder keywordBuilder = new KeywordFieldMapper.Builder(name);
        KeywordFieldMapper.KeywordFieldType keywordFieldType = keywordBuilder.buildFieldType(context, new FieldType());
        return new KeywordFieldMapper(
            name,
            new FieldType(),
            keywordFieldType,
            keywordBuilder.multiFieldsBuilder.build(keywordBuilder, context),
            keywordBuilder.copyTo.build(),
            keywordBuilder
        );
    },
        name -> o -> { throw new OpenSearchException("Cannot create IndexableField to execute queries on object derived field"); },
        formatter -> o -> o
    );

    final String name;
    private final TriFunction<String, Mapper.BuilderContext, IndexAnalyzers, FieldMapper> builder;

    private final Function<String, Function<Object, IndexableField>> indexableFieldBuilder;

    private final Function<DateFormatter, Function<Object, Object>> valueForDisplay;

    DerivedFieldSupportedTypes(
        String name,
        TriFunction<String, Mapper.BuilderContext, IndexAnalyzers, FieldMapper> builder,
        Function<String, Function<Object, IndexableField>> indexableFieldBuilder,
        Function<DateFormatter, Function<Object, Object>> valueForDisplay
    ) {
        this.name = name;
        this.builder = builder;
        this.indexableFieldBuilder = indexableFieldBuilder;
        this.valueForDisplay = valueForDisplay;
    }

    public String getName() {
        return name;
    }

    private FieldMapper getFieldMapper(String name, Mapper.BuilderContext context, IndexAnalyzers indexAnalyzers) {
        return builder.apply(name, context, indexAnalyzers);
    }

    private Function<Object, IndexableField> getIndexableFieldGenerator(String name) {
        return indexableFieldBuilder.apply(name);
    }

    private Function<Object, Object> getValueForDisplayGenerator(DateFormatter formatter) {
        return valueForDisplay.apply(formatter);
    }

    private static final Map<String, DerivedFieldSupportedTypes> enumMap = Arrays.stream(DerivedFieldSupportedTypes.values())
        .collect(Collectors.toMap(DerivedFieldSupportedTypes::getName, enumValue -> enumValue));

    @SuppressWarnings("checkstyle:DescendantToken")
    public static FieldMapper getFieldMapperFromType(
        String type,
        String name,
        Mapper.BuilderContext context,
        IndexAnalyzers indexAnalyzers
    ) {
        if (!enumMap.containsKey(type)) {
            throw new IllegalArgumentException("Type [" + type + "] isn't supported in Derived field context.");
        }
        return enumMap.get(type).getFieldMapper(name, context, indexAnalyzers);
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    public static Function<Object, IndexableField> getIndexableFieldGeneratorType(String type, String name) {
        if (!enumMap.containsKey(type)) {
            throw new IllegalArgumentException("Type [" + type + "] isn't supported in Derived field context.");
        }
        return enumMap.get(type).getIndexableFieldGenerator(name);
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    public static Function<Object, Object> getValueForDisplayGenerator(String type, DateFormatter formatter) {
        if (!enumMap.containsKey(type)) {
            throw new IllegalArgumentException("Type [" + type + "] isn't supported in Derived field context.");
        }
        return enumMap.get(type).getValueForDisplayGenerator(formatter);
    }
}
