package graphql.execution.instrumentation.fieldvalidation;

import graphql.GraphQLError;
import graphql.PublicApi;
import graphql.execution.ExecutionPath;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * This very simple field validation will run the supplied function for a given field path and if it returns an error
 * it will be added to the list of problems.
 *
 * Use {@link #addRule(graphql.execution.ExecutionPath, java.util.function.BiFunction)} to supply the rule callbacks where
 * you implement your specific business logic
 */
@PublicApi
public class SimpleFieldValidation implements FieldValidation {

    private final Map<ExecutionPath, BiFunction<FieldAndArguments, FieldValidationEnvironment, Optional<GraphQLError>>> rules = new LinkedHashMap<>();

    /**
     * Adds the rule against the field address path.  If the rule returns an error, it will be added to the list of errors
     *
     * @param fieldPath the path to the field
     * @param rule      the rule function
     *
     * @return this validator
     */
    public SimpleFieldValidation addRule(ExecutionPath fieldPath, BiFunction<FieldAndArguments, FieldValidationEnvironment, Optional<GraphQLError>> rule) {
        rules.put(fieldPath, rule);
        return this;
    }

    @Override
    public List<GraphQLError> validateFields(FieldValidationEnvironment validationEnvironment) {
        List<GraphQLError> errors = new ArrayList<>();
        for (ExecutionPath fieldPath : rules.keySet()) {
            List<FieldAndArguments> fieldAndArguments = validationEnvironment.getFieldsByPath().get(fieldPath);
            if (fieldAndArguments != null) {
                BiFunction<FieldAndArguments, FieldValidationEnvironment, Optional<GraphQLError>> ruleFunction = rules.get(fieldPath);

                for (FieldAndArguments fieldAndArgument : fieldAndArguments) {
                    Optional<GraphQLError> graphQLError = ruleFunction.apply(fieldAndArgument, validationEnvironment);
                    graphQLError.ifPresent(errors::add);
                }
            }
        }
        return errors;
    }
}
