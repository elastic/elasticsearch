package graphql.language;

import graphql.Assert;
import graphql.AssertException;
import graphql.GraphQLException;
import graphql.Internal;
import graphql.Scalars;
import graphql.parser.Parser;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.util.FpKit;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.schema.GraphQLTypeUtil.isList;
import static graphql.schema.GraphQLTypeUtil.isNonNull;

@Internal
public class AstValueHelper {

    /**
     * Produces a GraphQL Value AST given a Java value.
     *
     * A GraphQL type must be provided, which will be used to interpret different
     * Java values.
     *
     * <pre>
     * |      Value    | GraphQL Value        |
     * | ------------- | -------------------- |
     * | Object        | Input Object         |
     * | Array         | List                 |
     * | Boolean       | Boolean              |
     * | String        | String / Enum Value  |
     * | Number        | Int / Float          |
     * | Mixed         | Enum Value           |
     * </pre>
     *
     * @param value - the java value to be converted into graphql ast
     * @param type  the graphql type of the object
     *
     * @return a grapql language ast {@link Value}
     */
    public static Value astFromValue(Object value, GraphQLType type) {
        if (value == null) {
            return null;
        }

        if (isNonNull(type)) {
            return handleNonNull(value, (GraphQLNonNull) type);
        }

        // Convert JavaScript array to GraphQL list. If the GraphQLType is a list, but
        // the value is not an array, convert the value using the list's item type.
        if (isList(type)) {
            return handleList(value, (GraphQLList) type);
        }

        // Populate the fields of the input object by creating ASTs from each value
        // in the JavaScript object according to the fields in the input type.
        if (type instanceof GraphQLInputObjectType) {
            return handleInputObject(value, (GraphQLInputObjectType) type);
        }

        if (!(type instanceof GraphQLScalarType || type instanceof GraphQLEnumType)) {
            throw new AssertException("Must provide Input Type, cannot use: " + type.getClass());
        }

        // Since value is an internally represented value, it must be serialized
        // to an externally represented value before converting into an AST.
        final Object serialized = serialize(type, value);
        if (isNullish(serialized)) {
            return null;
        }

        // Others serialize based on their corresponding JavaScript scalar types.
        if (serialized instanceof Boolean) {
            return BooleanValue.newBooleanValue().value((Boolean) serialized).build();
        }

        String stringValue = serialized.toString();
        // numbers can be Int or Float values.
        if (serialized instanceof Number) {
            return handleNumber(stringValue);
        }

        if (serialized instanceof String) {
            // Enum types use Enum literals.
            if (type instanceof GraphQLEnumType) {
                return EnumValue.newEnumValue().name(stringValue).build();
            }

            // ID types can use Int literals.
            if (type == Scalars.GraphQLID && stringValue.matches("^[0-9]+$")) {
                return IntValue.newIntValue().value(new BigInteger(stringValue)).build();
            }

            // String types are just strings but JSON'ised
            return StringValue.newStringValue().value(jsonStringify(stringValue)).build();
        }

        throw new AssertException("'Cannot convert value to AST: " + serialized);
    }

    private static Value handleInputObject(Object _value, GraphQLInputObjectType type) {
        Map mapValue = objToMap(_value);
        List<GraphQLInputObjectField> fields = type.getFields();
        List<ObjectField> fieldNodes = new ArrayList<>();
        fields.forEach(field -> {
            GraphQLInputType fieldType = field.getType();
            Value nodeValue = astFromValue(mapValue.get(field.getName()), fieldType);
            if (nodeValue != null) {

                fieldNodes.add(ObjectField.newObjectField().name(field.getName()).value(nodeValue).build());
            }
        });
        return ObjectValue.newObjectValue().objectFields(fieldNodes).build();
    }

    private static Value handleNumber(String stringValue) {
        if (stringValue.matches("^[0-9]+$")) {
            return IntValue.newIntValue().value(new BigInteger(stringValue)).build();
        } else {
            return FloatValue.newFloatValue().value(new BigDecimal(stringValue)).build();
        }
    }

    private static Value handleList(Object _value, GraphQLList type) {
        GraphQLType itemType = type.getWrappedType();
        boolean isIterable = _value instanceof Iterable;
        if (isIterable || (_value != null && _value.getClass().isArray())) {
            Iterable iterable = isIterable ? (Iterable) _value : FpKit.toCollection(_value);
            List<Value> valuesNodes = new ArrayList<>();
            for (Object item : iterable) {
                Value itemNode = astFromValue(item, itemType);
                if (itemNode != null) {
                    valuesNodes.add(itemNode);
                }
            }
            return ArrayValue.newArrayValue().values(valuesNodes).build();
        }
        return astFromValue(_value, itemType);
    }

    private static Value handleNonNull(Object _value, GraphQLNonNull type) {
        GraphQLType wrappedType = type.getWrappedType();
        return astFromValue(_value, wrappedType);
    }

    /**
     * Encodes the value as a JSON string according to http://json.org/ rules
     *
     * @param stringValue the value to encode as a JSON string
     *
     * @return the encoded string
     */
    static String jsonStringify(String stringValue) {
        StringBuilder sb = new StringBuilder();
        for (char ch : stringValue.toCharArray()) {
            switch (ch) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '/':
                    sb.append("\\/");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static Object serialize(GraphQLType type, Object value) {
        if (type instanceof GraphQLScalarType) {
            return ((GraphQLScalarType) type).getCoercing().serialize(value);
        } else {
            return ((GraphQLEnumType) type).getCoercing().serialize(value);
        }
    }

    private static boolean isNullish(Object serialized) {
        if (serialized instanceof Number) {
            return Double.isNaN(((Number) serialized).doubleValue());
        }
        return serialized == null;
    }

    private static Map objToMap(Object value) {
        if (value instanceof Map) {
            return (Map) value;
        }
        // java bean inspector
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            BeanInfo info = Introspector.getBeanInfo(value.getClass());
            for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
                Method reader = pd.getReadMethod();
                if (reader != null)
                    result.put(pd.getName(), reader.invoke(value));
            }
        } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
            throw new GraphQLException(e);
        }
        return result;
    }

    /**
     * Parses an AST value literal into the correct {@link graphql.language.Value} which
     * MUST be of the correct shape eg '"string"' or 'true' or '1' or '{ "object", "form" }'
     * or '[ "array", "form" ]' otherwise an exception is thrown
     *
     * @param astLiteral the string to parse an AST literal
     *
     * @return a valid Value
     *
     * @throws graphql.AssertException if the input can be parsed
     */
    public static Value valueFromAst(String astLiteral) {
        // we use the parser to give us the AST elements as if we defined an inputType
        String toParse = "input X { x : String = " + astLiteral + "}";
        try {
            Document doc = new Parser().parseDocument(toParse);
            InputObjectTypeDefinition inputType = (InputObjectTypeDefinition) doc.getDefinitions().get(0);
            InputValueDefinition inputValueDefinition = inputType.getInputValueDefinitions().get(0);
            return inputValueDefinition.getDefaultValue();
        } catch (Exception e) {
            return Assert.assertShouldNeverHappen("valueFromAst of '%s' failed because of '%s'", astLiteral, e.getMessage());
        }
    }
}
