package graphql.validation.rules;


import graphql.execution.TypeFromAST;
import graphql.language.OperationDefinition;
import graphql.language.VariableDefinition;
import graphql.language.VariableReference;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

import java.util.LinkedHashMap;
import java.util.Map;

public class VariableTypesMatchRule extends AbstractRule {

    final VariablesTypesMatcher variablesTypesMatcher;

    private Map<String, VariableDefinition> variableDefinitionMap;

    public VariableTypesMatchRule(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        this(validationContext, validationErrorCollector, new VariablesTypesMatcher());
    }

    VariableTypesMatchRule(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector, VariablesTypesMatcher variablesTypesMatcher) {
        super(validationContext, validationErrorCollector);
        setVisitFragmentSpreads(true);
        this.variablesTypesMatcher = variablesTypesMatcher;
    }

    @Override
    public void checkOperationDefinition(OperationDefinition operationDefinition) {
        variableDefinitionMap = new LinkedHashMap<>();
    }

    @Override
    public void checkVariableDefinition(VariableDefinition variableDefinition) {
        variableDefinitionMap.put(variableDefinition.getName(), variableDefinition);
    }

    @Override
    public void checkVariable(VariableReference variableReference) {
        VariableDefinition variableDefinition = variableDefinitionMap.get(variableReference.getName());
        if (variableDefinition == null) {
            return;
        }
        GraphQLType variableType = TypeFromAST.getTypeFromAST(getValidationContext().getSchema(), variableDefinition.getType());
        if (variableType == null) {
            return;
        }
        GraphQLInputType expectedType = getValidationContext().getInputType();
        if (expectedType == null) {
            // we must have a unknown variable say to not have a known type
            return;
        }
        if (!variablesTypesMatcher.doesVariableTypesMatch(variableType, variableDefinition.getDefaultValue(), expectedType)) {
            GraphQLType effectiveType = variablesTypesMatcher.effectiveType(variableType, variableDefinition.getDefaultValue());
            String message = String.format("Variable type '%s' doesn't match expected type '%s'",
                    GraphQLTypeUtil.simplePrint(effectiveType),
                    GraphQLTypeUtil.simplePrint(expectedType));
            addError(ValidationErrorType.VariableTypeMismatch, variableReference.getSourceLocation(), message);
        }
    }


}
