package graphql;


import graphql.language.SourceLocation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The interface describing graphql errors
 *
 * NOTE: This class implements {@link java.io.Serializable} and hence it can be serialised and placed into a distributed cache.  However we
 * are not aiming to provide long term compatibility and do not intend for you to place this serialised data into permanent storage,
 * with times frames that cross graphql-java versions.  While we don't change things unnecessarily,  we may inadvertently break
 * the serialised compatibility across versions.
 *
 * @see <a href="https://facebook.github.io/graphql/#sec-Errors">GraphQL Spec - 7.2.2 Errors</a>
 */
@PublicApi
public interface GraphQLError extends Serializable {

    /**
     * @return a description of the error intended for the developer as a guide to understand and correct the error
     */
    String getMessage();

    /**
     * @return the location(s) within the GraphQL document at which the error occurred. Each {@link SourceLocation}
     * describes the beginning of an associated syntax element
     */
    List<SourceLocation> getLocations();

    /**
     * @return an object classifying this error
     */
    ErrorClassification getErrorType();

    /**
     * The graphql spec says that the (optional) path field of any error should be a list
     * of path entries - http://facebook.github.io/graphql/#sec-Errors
     *
     * @return the path in list format
     */
    default List<Object> getPath() {
        return null;
    }

    /**
     * The graphql specification says that result of a call should be a map that follows certain rules on what items
     * should be present.  Certain JSON serializers may or may interpret the error to spec, so this method
     * is provided to produce a map that strictly follows the specification.
     *
     * See : <a href="http://facebook.github.io/graphql/#sec-Errors">http://facebook.github.io/graphql/#sec-Errors</a>
     *
     * @return a map of the error that strictly follows the specification
     */
    default Map<String, Object> toSpecification() {
        return GraphqlErrorHelper.toSpecification(this);
    }

    /**
     * @return a map of error extensions or null if there are none
     */
    default Map<String, Object> getExtensions() {
        return null;
    }


}
