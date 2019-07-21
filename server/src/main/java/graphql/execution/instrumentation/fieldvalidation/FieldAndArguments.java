package graphql.execution.instrumentation.fieldvalidation;

import graphql.PublicApi;
import graphql.execution.ExecutionPath;
import graphql.language.Field;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLFieldDefinition;

import java.util.Map;

/**
 * This represents a field and its arguments that may be validated.
 */
@PublicApi
public interface FieldAndArguments {

    /**
     * @return the field in play
     */
    Field getField();

    /**
     * @return the runtime type definition of the field
     */
    GraphQLFieldDefinition getFieldDefinition();

    /**
     * @return the containing type of the field
     */
    GraphQLCompositeType getParentType();

    /**
     * @return the parent arguments or null if there is no parent
     */
    FieldAndArguments getParentFieldAndArguments();

    /**
     * @return the path to this field
     */
    ExecutionPath getPath();

    /**
     * This will be a map of argument names to argument values.  This will contain any variables transferred
     * along with any default values ready for execution.  This is what you use to do most of your validation against
     *
     * @return a map of argument names to values
     */
    Map<String, Object> getArgumentValuesByName();

    /**
     * This will return the named field argument value and cast it to the desired type.
     *
     * @param argumentName the name of the argument
     * @param <T>          the type of the underlying value object
     *
     * @return a cast object of type T
     */
    @SuppressWarnings("TypeParameterUnusedInFormals")
    <T> T getArgumentValue(String argumentName);
}
