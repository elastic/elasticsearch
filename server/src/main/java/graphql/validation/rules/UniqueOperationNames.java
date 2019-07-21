package graphql.validation.rules;

import graphql.language.OperationDefinition;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A GraphQL document is only valid if all defined operations have unique names.
 * http://facebook.github.io/graphql/October2016/#sec-Operation-Name-Uniqueness
 */
public class UniqueOperationNames extends AbstractRule {

    private Set<String> operationNames = new LinkedHashSet<>();

    public UniqueOperationNames(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    public void checkOperationDefinition(OperationDefinition operationDefinition) {
        super.checkOperationDefinition(operationDefinition);
        String name = operationDefinition.getName();

        // skip validation for anonymous operations
        if (name == null) {
            return;
        }

        if (operationNames.contains(name)) {
            addError(ValidationErrorType.DuplicateOperationName, operationDefinition.getSourceLocation(), duplicateOperationNameMessage(name));
        } else {
            operationNames.add(name);
        }
    }

    static String duplicateOperationNameMessage(String definitionName) {
        return String.format("There can be only one operation named '%s'", definitionName);
    }
}
