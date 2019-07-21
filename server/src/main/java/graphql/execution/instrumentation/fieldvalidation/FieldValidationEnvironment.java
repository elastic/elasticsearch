package graphql.execution.instrumentation.fieldvalidation;

import graphql.GraphQLError;
import graphql.PublicApi;
import graphql.execution.ExecutionContext;
import graphql.execution.ExecutionPath;

import java.util.List;
import java.util.Map;

/**
 * This contains all of the field and their arguments for a given query.  The method
 * {@link #getFieldsByPath()} will be where most of the useful validation information is
 * contained.  It also gives you a helper to make validation error messages.
 *
 * One thing to note is that because queries can have repeating fragments, the same
 * logical field can appear multiple times with different input values.  That is
 * why {@link #getFieldsByPath()} returns a list of fields and their arguments.
 * if you don't have fragments then the list will be of size 1
 *
 * @see FieldAndArguments
 */
@PublicApi
public interface FieldValidationEnvironment {

    /**
     * @return the schema in play
     */
    ExecutionContext getExecutionContext();

    /**
     * @return a list of {@link FieldAndArguments}
     */
    List<FieldAndArguments> getFields();

    /**
     * @return a map of field paths to {@link FieldAndArguments}
     */
    Map<ExecutionPath, List<FieldAndArguments>> getFieldsByPath();

    /**
     * This helper method allows you to make error messages to be passed back out in case of validation failure.  Note you
     * don't NOT have to use this helper.  Any implementation of {@link graphql.GraphQLError} is valid
     *
     * @param msg the error message
     *
     * @return a graphql error
     */
    GraphQLError mkError(String msg);

    /**
     * This helper method allows you to make error messages to be passed back out in case of validation failure.  Note you
     * don't NOT have to use this helper.  Any implementation of {@link graphql.GraphQLError} is valid
     *
     * @param msg               the error message
     * @param fieldAndArguments the field in error
     *
     * @return a graphql error
     */
    GraphQLError mkError(String msg, FieldAndArguments fieldAndArguments);
}
