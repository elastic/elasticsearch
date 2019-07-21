package graphql.validation.rules;

import graphql.language.Definition;
import graphql.language.Document;
import graphql.language.FragmentDefinition;
import graphql.language.OperationDefinition;
import graphql.language.SchemaDefinition;
import graphql.language.TypeDefinition;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

public class ExecutableDefinitions extends AbstractRule {

    public ExecutableDefinitions(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    /**
     * Executable definitions
     *
     * A GraphQL document is only valid for execution if all definitions are either
     * operation or fragment definitions.
     */
    @Override
    public void checkDocument(Document document) {
        document.getDefinitions().forEach(definition -> {
            if (!(definition instanceof OperationDefinition)
                && !(definition instanceof FragmentDefinition)) {

                String message = nonExecutableDefinitionMessage(definition);
                addError(ValidationErrorType.NonExecutableDefinition, definition.getSourceLocation(), message);
            }
        });
    }

    private String nonExecutableDefinitionMessage(Definition definition) {

        String definitionName;
        if (definition instanceof TypeDefinition) {
            definitionName = ((TypeDefinition) definition).getName();
        } else if (definition instanceof SchemaDefinition) {
            definitionName = "schema";
        } else {
            definitionName = "provided";
        }

        return nonExecutableDefinitionMessage(definitionName);
    }

    static String nonExecutableDefinitionMessage(String definitionName) {
        return String.format("The %s definition is not executable.", definitionName);
    }
}
