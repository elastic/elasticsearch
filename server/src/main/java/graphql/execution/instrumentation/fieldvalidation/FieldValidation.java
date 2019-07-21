package graphql.execution.instrumentation.fieldvalidation;

import graphql.GraphQLError;
import graphql.PublicSpi;

import java.util.List;

/**
 * This pluggable interface allows you to validate the fields and their argument inputs before query execution.
 *
 * You will be called with fields and their arguments expanded out ready for execution and you can check business logic
 * concerns like the lengths of input objects (eg an input string cant be longer than say 255 chars) or that the
 * input objects have a certain shape that is required for this query.
 *
 * You are only called once with all the field information expanded out for you.  This allows you to set up cross field business rules,
 * for example if field argument X has a value then field Y argument must also have a value say.
 *
 * @see FieldValidationEnvironment
 * @see SimpleFieldValidation
 */
@PublicSpi
public interface FieldValidation {

    /**
     * This is called to validate the fields and their arguments
     *
     * @param validationEnvironment the validation environment
     *
     * @return a list of errors.  If this is non empty then the query will not execute.
     */
    List<GraphQLError> validateFields(FieldValidationEnvironment validationEnvironment);
}
