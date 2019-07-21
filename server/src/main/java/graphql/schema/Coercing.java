package graphql.schema;


import graphql.PublicSpi;

import java.util.Map;

/**
 * The Coercing interface is used by {@link graphql.schema.GraphQLScalarType}s to parse and serialise object values.
 * <p>
 * There are two major responsibilities, result coercion and input coercion.
 * <p>
 * Result coercion is taking a value from a Java object and coercing it into the constraints of the scalar type.
 * For example imagine a DateTime scalar, the result coercion would need to take an object and turn it into a
 * ISO date or throw an exception if it cant.
 * <p>
 * Input coercion is taking a value that came in from requests variables or hard coded query literals and coercing them into a
 * Java object value that is acceptable to the scalar type.  Again using the DateTime example, the input coercion would try to
 * parse an ISO date time object or throw an exception if it cant.
 *
 * See http://facebook.github.io/graphql/#sec-Scalars
 */
@PublicSpi
public interface Coercing<I, O> {

    /**
     * Called to convert a Java object result of a DataFetcher to a valid runtime value for the scalar type.
     * <p>
     * Note : Throw {@link graphql.schema.CoercingSerializeException} if there is fundamental
     * problem during serialisation, don't return null to indicate failure.
     * <p>
     * Note : You should not allow {@link java.lang.RuntimeException}s to come out of your serialize method, but rather
     * catch them and fire them as {@link graphql.schema.CoercingSerializeException} instead as per the method contract.
     *
     * @param dataFetcherResult is never null
     *
     * @return a serialized value which may be null.
     *
     * @throws graphql.schema.CoercingSerializeException if value input can't be serialized
     */
    O serialize(Object dataFetcherResult) throws CoercingSerializeException;

    /**
     * Called to resolve a input from a query variable into a Java object acceptable for the scalar type.
     * <p>
     * Note : You should not allow {@link java.lang.RuntimeException}s to come out of your parseValue method, but rather
     * catch them and fire them as {@link graphql.schema.CoercingSerializeException} instead as per the method contract.
     *
     * @param input is never null
     *
     * @return a parsed value which is never null
     *
     * @throws graphql.schema.CoercingParseValueException if value input can't be parsed
     */
    I parseValue(Object input) throws CoercingParseValueException;

    /**
     * Called during query validation to convert an query input AST node into a Java object acceptable for the scalar type.  The input
     * object will be an instance of {@link graphql.language.Value}.
     * <p>
     * Note : You should not allow {@link java.lang.RuntimeException}s to come out of your parseLiteral method, but rather
     * catch them and fire them as {@link graphql.schema.CoercingParseLiteralException} instead as per the method contract.
     *
     * @param input is never null
     *
     * @return a parsed value which is never null
     *
     * @throws graphql.schema.CoercingParseLiteralException if input literal can't be parsed
     */
    I parseLiteral(Object input) throws CoercingParseLiteralException;

    /**
     * Called during query execution to convert an query input AST node into a Java object acceptable for the scalar type.  The input
     * object will be an instance of {@link graphql.language.Value}.
     * <p>
     * Note : You should not allow {@link java.lang.RuntimeException}s to come out of your parseLiteral method, but rather
     * catch them and fire them as {@link graphql.schema.CoercingParseLiteralException} instead as per the method contract.
     * <p>
     * Many scalar types don't need to implement this method because they don't take AST {@link graphql.language.VariableReference}
     * objects and convert them into actual values.  But for those scalar types that want to do this, then this
     * method should be implemented.
     *
     * @param input     is never null
     * @param variables the resolved variables passed to the query
     *
     * @return a parsed value which is never null
     *
     * @throws graphql.schema.CoercingParseLiteralException if input literal can't be parsed
     */
    @SuppressWarnings("unused")
    default I parseLiteral(Object input, Map<String, Object> variables) throws CoercingParseLiteralException {
        return parseLiteral(input);
    }

    ;
}
