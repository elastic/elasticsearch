package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public final class MultiValuesSourceParseHelper {

    public static <VS extends ValuesSource, T> void declareCommon(
        AbstractObjectParser<? extends MultiValuesSourceAggregationBuilder<VS, ?>, T> objectParser, boolean formattable,
        ValueType targetValueType) {

        objectParser.declareField(MultiValuesSourceAggregationBuilder::valueType, p -> {
            ValueType valueType = ValueType.resolveForScript(p.text());
            if (targetValueType != null && valueType.isNotA(targetValueType)) {
                throw new ParsingException(p.getTokenLocation(),
                    "Aggregation [" + objectParser.getName() + "] was configured with an incompatible value type ["
                        + valueType + "]. It can only work on value of type ["
                        + targetValueType + "]");
            }
            return valueType;
        }, ValueType.VALUE_TYPE, ObjectParser.ValueType.STRING);

        if (formattable) {
            objectParser.declareField(MultiValuesSourceAggregationBuilder::format, XContentParser::text,
                ParseField.CommonFields.FORMAT, ObjectParser.ValueType.STRING);
        }
    }

    public static <VS extends ValuesSource, T> void declareField(String fieldName,
        AbstractObjectParser<? extends MultiValuesSourceAggregationBuilder<VS, ?>, T> objectParser,
        boolean scriptable, boolean timezoneAware) {

        objectParser.declareField((o, fieldConfig) -> o.field(fieldName, fieldConfig),
            (p, c) -> MultiValuesSourceFieldConfig.PARSER.apply(scriptable, timezoneAware).parse(p, null),
            new ParseField(fieldName), ObjectParser.ValueType.OBJECT);
    }
}
