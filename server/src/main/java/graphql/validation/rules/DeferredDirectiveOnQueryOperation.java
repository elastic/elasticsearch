package graphql.validation.rules;

import graphql.Internal;
import graphql.language.Directive;
import graphql.language.Node;
import graphql.language.OperationDefinition;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

import java.util.List;
import java.util.Optional;

@Internal
public class DeferredDirectiveOnQueryOperation extends DeferredDirectiveAbstractRule {

    public DeferredDirectiveOnQueryOperation(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    protected void onDeferredDirective(Directive deferredDirective, List<Node> ancestors, GraphQLCompositeType parentType, GraphQLFieldDefinition fieldDef) {
        Optional<OperationDefinition> operationDefinition = getOperation(ancestors);
        if (operationDefinition.isPresent()) {
            OperationDefinition.Operation operation = operationDefinition.get().getOperation();
            if (operation != OperationDefinition.Operation.QUERY) {
                String message = String.format("@defer directives can only be applied to QUERY operations and not '%s' operations - %s.%s ", operation, parentType.getName(), fieldDef.getName());
                addError(ValidationErrorType.DeferDirectiveNotOnQueryOperation, deferredDirective.getSourceLocation(), message);
            }
        }
    }

    private Optional<OperationDefinition> getOperation(List<Node> ancestors) {
        return ancestors.stream()
                .filter(def -> def instanceof OperationDefinition)
                .map((def -> (OperationDefinition) def))
                .findFirst();
    }
}
