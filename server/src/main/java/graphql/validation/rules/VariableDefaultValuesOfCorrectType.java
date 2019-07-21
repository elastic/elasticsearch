package graphql.validation.rules;

import graphql.language.VariableDefinition;
import graphql.schema.GraphQLInputType;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

import static graphql.schema.GraphQLTypeUtil.isNonNull;


public class VariableDefaultValuesOfCorrectType extends AbstractRule {


    public VariableDefaultValuesOfCorrectType(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }


    @Override
    public void checkVariableDefinition(VariableDefinition variableDefinition) {
        GraphQLInputType inputType = getValidationContext().getInputType();
        if (inputType == null) return;
        if (isNonNull(inputType) && variableDefinition.getDefaultValue() != null) {
            String message = "Missing value for non null type";
            addError(ValidationErrorType.DefaultForNonNullArgument, variableDefinition.getSourceLocation(), message);
        }
        if (variableDefinition.getDefaultValue() != null
                && !getValidationUtil().isValidLiteralValue(variableDefinition.getDefaultValue(), inputType, getValidationContext().getSchema())) {
            String message = String.format("Bad default value %s for type %s", variableDefinition.getDefaultValue(), inputType.getName());
            addError(ValidationErrorType.BadValueForDefaultArg, variableDefinition.getSourceLocation(), message);
        }
    }
}
