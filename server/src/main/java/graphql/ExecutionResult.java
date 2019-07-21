package graphql;


import java.util.List;
import java.util.Map;

/**
 * This simple value class represents the result of performing a graphql query.
 */
@PublicApi
@SuppressWarnings("TypeParameterUnusedInFormals")
public interface ExecutionResult {

    /**
     * @return the errors that occurred during execution or empty list if there is none
     */
    List<GraphQLError> getErrors();

    /**
     * @param <T> allows type coercion
     *
     * @return the data in the result or null if there is none
     */
    <T> T getData();

    /**
     * The graphql specification specifies:
     *
     * "If an error was encountered before execution begins, the data entry should not be present in the result.
     * If an error was encountered during the execution that prevented a valid response, the data entry in the response should be null."
     *
     * This allows to distinguish between the cases where {@link #getData()} returns null.
     *
     * See : <a href="https://graphql.github.io/graphql-spec/June2018/#sec-Data">https://graphql.github.io/graphql-spec/June2018/#sec-Data</a>
     *
     * @return <code>true</code> if the entry "data" should be present in the result
     *         <code>false</code> otherwise
     */
    boolean isDataPresent();

    /**
     * @return a map of extensions or null if there are none
     */
    Map<Object, Object> getExtensions();


    /**
     * The graphql specification says that result of a call should be a map that follows certain rules on what items
     * should be present.  Certain JSON serializers may or may interpret {@link ExecutionResult} to spec, so this method
     * is provided to produce a map that strictly follows the specification.
     *
     * See : <a href="http://facebook.github.io/graphql/#sec-Response-Format">http://facebook.github.io/graphql/#sec-Response-Format</a>
     *
     * @return a map of the result that strictly follows the spec
     */
    Map<String, Object> toSpecification();
}
