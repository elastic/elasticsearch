package graphql;


import graphql.language.BooleanValue;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.StringValue;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import static graphql.Assert.assertShouldNeverHappen;

/**
 * This contains the implementations of the Scalar types that ship with graphql-java.  Some are proscribed
 * by the graphql specification (Int, Float, String, Boolean and ID) while others are offer because they are common on
 * Java platforms.
 *
 * For more info see http://graphql.org/learn/schema/#scalar-types and more specifically http://facebook.github.io/graphql/#sec-Scalars
 */
public class Scalars {

    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger INT_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger BYTE_MAX = BigInteger.valueOf(Byte.MAX_VALUE);
    private static final BigInteger BYTE_MIN = BigInteger.valueOf(Byte.MIN_VALUE);
    private static final BigInteger SHORT_MAX = BigInteger.valueOf(Short.MAX_VALUE);
    private static final BigInteger SHORT_MIN = BigInteger.valueOf(Short.MIN_VALUE);


    private static boolean isNumberIsh(Object input) {
        return input instanceof Number || input instanceof String;
    }

    private static String typeName(Object input) {
        if (input == null) {
            return "null";
        }

        return input.getClass().getSimpleName();
    }

    /**
     * This represents the "Int" type as defined in the graphql specification : http://facebook.github.io/graphql/#sec-Int
     *
     * The Int scalar type represents a signed 32‐bit numeric non‐fractional value.
     */
    public static final GraphQLScalarType GraphQLInt = new GraphQLScalarType("Int", "Built-in Int", new Coercing<Integer, Integer>() {

        private Integer convertImpl(Object input) {
            if (input instanceof Integer) {
                return (Integer) input;
            } else if (isNumberIsh(input)) {
                BigDecimal value;
                try {
                    value = new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
                try {
                    return value.intValueExact();
                } catch (ArithmeticException e) {
                    return null;
                }
            } else {
                return null;
            }
        }

        @Override
        public Integer serialize(Object input) {
            Integer result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'Int' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Integer parseValue(Object input) {
            Integer result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'Int' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Integer parseLiteral(Object input) {
            if (!(input instanceof IntValue)) {
                throw new CoercingParseLiteralException(
                        "Expected AST type 'IntValue' but was '" + typeName(input) + "'."
                );
            }
            BigInteger value = ((IntValue) input).getValue();
            if (value.compareTo(INT_MIN) < 0 || value.compareTo(INT_MAX) > 0) {
                throw new CoercingParseLiteralException(
                        "Expected value to be in the Integer range but it was '" + value.toString() + "'"
                );
            }
            return value.intValue();
        }
    });

    /**
     * This represents the "Float" type as defined in the graphql specification : http://facebook.github.io/graphql/#sec-Float
     *
     * Note: The Float type in GraphQL is equivalent to Double in Java. (double precision IEEE 754)
     */
    public static final GraphQLScalarType GraphQLFloat = new GraphQLScalarType("Float", "Built-in Float", new Coercing<Double, Double>() {

        private Double convertImpl(Object input) {
            if (isNumberIsh(input)) {
                BigDecimal value;
                try {
                    value = new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
                return value.doubleValue();
            } else {
                return null;
            }

        }

        @Override
        public Double serialize(Object input) {
            Double result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'Float' but was '" + typeName(input) + "'."
                );
            }
            return result;

        }

        @Override
        public Double parseValue(Object input) {
            Double result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'Float' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Double parseLiteral(Object input) {
            if (input instanceof IntValue) {
                return ((IntValue) input).getValue().doubleValue();
            } else if (input instanceof FloatValue) {
                return ((FloatValue) input).getValue().doubleValue();
            } else {
                throw new CoercingParseLiteralException(
                        "Expected AST type 'IntValue' or 'FloatValue' but was '" + typeName(input) + "'."
                );
            }
        }
    });

    /**
     * This represents the "String" type as defined in the graphql specification : http://facebook.github.io/graphql/#sec-String
     */
    public static final GraphQLScalarType GraphQLString = new GraphQLScalarType("String", "Built-in String", new Coercing<String, String>() {
        @Override
        public String serialize(Object input) {
            return input.toString();
        }

        @Override
        public String parseValue(Object input) {
            return serialize(input);
        }

        @Override
        public String parseLiteral(Object input) {
            if (!(input instanceof StringValue)) {
                throw new CoercingParseLiteralException(
                        "Expected AST type 'StringValue' but was '" + typeName(input) + "'."
                );
            }
            return ((StringValue) input).getValue();
        }
    });

    /**
     * This represents the "Boolean" type as defined in the graphql specification : http://facebook.github.io/graphql/#sec-Boolean
     */
    public static final GraphQLScalarType GraphQLBoolean = new GraphQLScalarType("Boolean", "Built-in Boolean", new Coercing<Boolean, Boolean>() {

        private Boolean convertImpl(Object input) {
            if (input instanceof Boolean) {
                return (Boolean) input;
            } else if (input instanceof String) {
                return Boolean.parseBoolean((String) input);
            } else if (isNumberIsh(input)) {
                BigDecimal value;
                try {
                    value = new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    // this should never happen because String is handled above
                    return assertShouldNeverHappen();
                }
                return value.compareTo(BigDecimal.ZERO) != 0;
            } else {
                return null;
            }

        }

        @Override
        public Boolean serialize(Object input) {
            Boolean result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'Boolean' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Boolean parseValue(Object input) {
            Boolean result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'Boolean' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Boolean parseLiteral(Object input) {
            if (!(input instanceof BooleanValue)) {
                throw new CoercingParseLiteralException(
                        "Expected AST type 'BooleanValue' but was '" + typeName(input) + "'."
                );
            }
            return ((BooleanValue) input).isValue();
        }
    });

    /**
     * This represents the "ID" type as defined in the graphql specification : http://facebook.github.io/graphql/#sec-ID
     *
     * The ID scalar type represents a unique identifier, often used to re-fetch an object or as the key for a cache. The
     * ID type is serialized in the same way as a String; however, it is not intended to be human‐readable. While it is
     * often numeric, it should always serialize as a String.
     */
    public static final GraphQLScalarType GraphQLID = new GraphQLScalarType("ID", "Built-in ID", new Coercing<Object, Object>() {

        private String convertImpl(Object input) {
            if (input instanceof String) {
                return (String) input;
            }
            if (input instanceof Integer) {
                return String.valueOf(input);
            }
            if (input instanceof Long) {
                return String.valueOf(input);
            }
            if (input instanceof UUID) {
                return String.valueOf(input);
            }
            return null;

        }

        @Override
        public String serialize(Object input) {
            String result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'ID' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public String parseValue(Object input) {
            String result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'ID' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public String parseLiteral(Object input) {
            if (input instanceof StringValue) {
                return ((StringValue) input).getValue();
            }
            if (input instanceof IntValue) {
                return ((IntValue) input).getValue().toString();
            }
            throw new CoercingParseLiteralException(
                    "Expected AST type 'IntValue' or 'StringValue' but was '" + typeName(input) + "'."
            );
        }
    });

    /**
     * This represents the "Long" type which is a representation of java.lang.Long
     */
    public static final GraphQLScalarType GraphQLLong = new GraphQLScalarType("Long", "Long type", new Coercing<Long, Long>() {

        private Long convertImpl(Object input) {
            if (input instanceof Long) {
                return (Long) input;
            } else if (isNumberIsh(input)) {
                BigDecimal value;
                try {
                    value = new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
                try {
                    return value.longValueExact();
                } catch (ArithmeticException e) {
                    return null;
                }
            } else {
                return null;
            }

        }

        @Override
        public Long serialize(Object input) {
            Long result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'Long' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Long parseValue(Object input) {
            Long result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'Long' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Long parseLiteral(Object input) {
            if (input instanceof StringValue) {
                try {
                    return Long.parseLong(((StringValue) input).getValue());
                } catch (NumberFormatException e) {
                    throw new CoercingParseLiteralException(
                            "Expected value to be a Long but it was '" + String.valueOf(input) + "'"
                    );
                }
            } else if (input instanceof IntValue) {
                BigInteger value = ((IntValue) input).getValue();
                if (value.compareTo(LONG_MIN) < 0 || value.compareTo(LONG_MAX) > 0) {
                    throw new CoercingParseLiteralException(
                            "Expected value to be in the Long range but it was '" + value.toString() + "'"
                    );
                }
                return value.longValue();
            }
            throw new CoercingParseLiteralException(
                    "Expected AST type 'IntValue' or 'StringValue' but was '" + typeName(input) + "'."
            );
        }
    });

    /**
     * This represents the "Short" type which is a representation of java.lang.Short
     */
    public static final GraphQLScalarType GraphQLShort = new GraphQLScalarType("Short", "Built-in Short as Int", new Coercing<Short, Short>() {

        private Short convertImpl(Object input) {
            if (input instanceof Short) {
                return (Short) input;
            } else if (isNumberIsh(input)) {
                BigDecimal value;
                try {
                    value = new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
                try {
                    return value.shortValueExact();
                } catch (ArithmeticException e) {
                    return null;
                }
            } else {
                return null;
            }

        }

        @Override
        public Short serialize(Object input) {
            Short result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'Short' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Short parseValue(Object input) {
            Short result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'Short' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Short parseLiteral(Object input) {
            if (!(input instanceof IntValue)) {
                throw new CoercingParseLiteralException(
                        "Expected AST type 'IntValue' but was '" + typeName(input) + "'."
                );
            }
            BigInteger value = ((IntValue) input).getValue();
            if (value.compareTo(SHORT_MIN) < 0 || value.compareTo(SHORT_MAX) > 0) {
                throw new CoercingParseLiteralException(
                        "Expected value to be in the Short range but it was '" + value.toString() + "'"
                );
            }
            return value.shortValue();
        }
    });

    /**
     * This represents the "Byte" type which is a representation of java.lang.Byte
     */
    public static final GraphQLScalarType GraphQLByte = new GraphQLScalarType("Byte", "Built-in Byte as Int", new Coercing<Byte, Byte>() {

        private Byte convertImpl(Object input) {
            if (input instanceof Byte) {
                return (Byte) input;
            } else if (isNumberIsh(input)) {
                BigDecimal value;
                try {
                    value = new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
                try {
                    return value.byteValueExact();
                } catch (ArithmeticException e) {
                    return null;
                }
            } else {
                return null;
            }

        }

        @Override
        public Byte serialize(Object input) {
            Byte result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'Byte' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Byte parseValue(Object input) {
            Byte result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'Byte' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Byte parseLiteral(Object input) {
            if (!(input instanceof IntValue)) {
                throw new CoercingParseLiteralException(
                        "Expected AST type 'IntValue' but was '" + typeName(input) + "'."
                );
            }
            BigInteger value = ((IntValue) input).getValue();
            if (value.compareTo(BYTE_MIN) < 0 || value.compareTo(BYTE_MAX) > 0) {
                throw new CoercingParseLiteralException(
                        "Expected value to be in the Byte range but it was '" + value.toString() + "'"
                );
            }
            return value.byteValue();
        }
    });


    /**
     * This represents the "BigInteger" type which is a representation of java.math.BigInteger
     */
    public static final GraphQLScalarType GraphQLBigInteger = new GraphQLScalarType("BigInteger", "Built-in java.math.BigInteger", new Coercing<BigInteger, BigInteger>() {

        private BigInteger convertImpl(Object input) {
            if (isNumberIsh(input)) {
                BigDecimal value;
                try {
                    value = new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
                try {
                    return value.toBigIntegerExact();
                } catch (ArithmeticException e) {
                    return null;
                }
            }
            return null;

        }

        @Override
        public BigInteger serialize(Object input) {
            BigInteger result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'BigInteger' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public BigInteger parseValue(Object input) {
            BigInteger result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'BigInteger' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public BigInteger parseLiteral(Object input) {
            if (input instanceof StringValue) {
                try {
                    return new BigDecimal(((StringValue) input).getValue()).toBigIntegerExact();
                } catch (NumberFormatException | ArithmeticException e) {
                    throw new CoercingParseLiteralException(
                            "Unable to turn AST input into a 'BigInteger' : '" + String.valueOf(input) + "'"
                    );
                }
            } else if (input instanceof IntValue) {
                return ((IntValue) input).getValue();
            } else if (input instanceof FloatValue) {
                try {
                    return ((FloatValue) input).getValue().toBigIntegerExact();
                } catch (ArithmeticException e) {
                    throw new CoercingParseLiteralException(
                            "Unable to turn AST input into a 'BigInteger' : '" + String.valueOf(input) + "'"
                    );
                }
            }
            throw new CoercingParseLiteralException(
                    "Expected AST type 'IntValue', 'StringValue' or 'FloatValue' but was '" + typeName(input) + "'."
            );
        }
    });

    /**
     * This represents the "BigDecimal" type which is a representation of java.math.BigDecimal
     */
    public static final GraphQLScalarType GraphQLBigDecimal = new GraphQLScalarType("BigDecimal", "Built-in java.math.BigDecimal", new Coercing<BigDecimal, BigDecimal>() {

        private BigDecimal convertImpl(Object input) {
            if (isNumberIsh(input)) {
                try {
                    return new BigDecimal(input.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            }
            return null;

        }

        @Override
        public BigDecimal serialize(Object input) {
            BigDecimal result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'BigDecimal' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public BigDecimal parseValue(Object input) {
            BigDecimal result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'BigDecimal' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public BigDecimal parseLiteral(Object input) {
            if (input instanceof StringValue) {
                try {
                    return new BigDecimal(((StringValue) input).getValue());
                } catch (NumberFormatException e) {
                    throw new CoercingParseLiteralException(
                            "Unable to turn AST input into a 'BigDecimal' : '" + String.valueOf(input) + "'"
                    );
                }
            } else if (input instanceof IntValue) {
                return new BigDecimal(((IntValue) input).getValue());
            } else if (input instanceof FloatValue) {
                return ((FloatValue) input).getValue();
            }
            throw new CoercingParseLiteralException(
                    "Expected AST type 'IntValue', 'StringValue' or 'FloatValue' but was '" + typeName(input) + "'."
            );
        }
    });


    /**
     * This represents the "Char" type which is a representation of java.lang.Character
     */
    public static final GraphQLScalarType GraphQLChar = new GraphQLScalarType("Char", "Built-in Char as Character", new Coercing<Character, Character>() {

        private Character convertImpl(Object input) {
            if (input instanceof String && ((String) input).length() == 1) {
                return ((String) input).charAt(0);
            } else if (input instanceof Character) {
                return (Character) input;
            } else {
                return null;
            }

        }

        @Override
        public Character serialize(Object input) {
            Character result = convertImpl(input);
            if (result == null) {
                throw new CoercingSerializeException(
                        "Expected type 'Char' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Character parseValue(Object input) {
            Character result = convertImpl(input);
            if (result == null) {
                throw new CoercingParseValueException(
                        "Expected type 'Char' but was '" + typeName(input) + "'."
                );
            }
            return result;
        }

        @Override
        public Character parseLiteral(Object input) {
            if (!(input instanceof StringValue)) {
                throw new CoercingParseLiteralException(
                        "Expected AST type 'StringValue' but was '" + typeName(input) + "'."
                );
            }
            String value = ((StringValue) input).getValue();
            if (value.length() != 1) {
                throw new CoercingParseLiteralException(
                        "Empty 'StringValue' provided."
                );
            }
            return value.charAt(0);
        }
    });
}
