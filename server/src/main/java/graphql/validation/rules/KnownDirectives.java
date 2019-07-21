package graphql.validation.rules;


import graphql.introspection.Introspection.DirectiveLocation;
import graphql.language.Directive;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Node;
import graphql.language.OperationDefinition;
import graphql.language.OperationDefinition.Operation;
import graphql.schema.GraphQLDirective;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

import java.util.List;

public class KnownDirectives extends AbstractRule {


    public KnownDirectives(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    public void checkDirective(Directive directive, List<Node> ancestors) {
        GraphQLDirective graphQLDirective = getValidationContext().getSchema().getDirective(directive.getName());
        if (graphQLDirective == null) {
            String message = String.format("Unknown directive %s", directive.getName());
            addError(ValidationErrorType.UnknownDirective, directive.getSourceLocation(), message);
            return;
        }

        Node ancestor = ancestors.get(ancestors.size() - 1);
        if (hasInvalidLocation(graphQLDirective, ancestor)) {
            String message = String.format("Directive %s not allowed here", directive.getName());
            addError(ValidationErrorType.MisplacedDirective, directive.getSourceLocation(), message);
        }
    }

    @SuppressWarnings("deprecation") // the suppression stands because its deprecated but still in graphql spec
    private boolean hasInvalidLocation(GraphQLDirective directive, Node ancestor) {
        if (ancestor instanceof OperationDefinition) {
            Operation operation = ((OperationDefinition) ancestor).getOperation();
            return Operation.QUERY.equals(operation) ?
                    !(directive.validLocations().contains(DirectiveLocation.QUERY) || directive.isOnOperation()) :
                    !(directive.validLocations().contains(DirectiveLocation.MUTATION) || directive.isOnOperation());
        } else if (ancestor instanceof Field) {
            return !(directive.validLocations().contains(DirectiveLocation.FIELD) || directive.isOnField());
        } else if (ancestor instanceof FragmentSpread) {
            return !(directive.validLocations().contains(DirectiveLocation.FRAGMENT_SPREAD) || directive.isOnFragment());
        } else if (ancestor instanceof FragmentDefinition) {
            return !(directive.validLocations().contains(DirectiveLocation.FRAGMENT_DEFINITION) || directive.isOnFragment());
        } else if (ancestor instanceof InlineFragment) {
            return !(directive.validLocations().contains(DirectiveLocation.INLINE_FRAGMENT) || directive.isOnFragment());
        }
        return true;
    }
}
