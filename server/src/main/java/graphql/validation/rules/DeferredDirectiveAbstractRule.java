package graphql.validation.rules;

import graphql.Directives;
import graphql.Internal;
import graphql.language.Directive;
import graphql.language.Node;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;

import java.util.List;

@Internal
public abstract class DeferredDirectiveAbstractRule extends AbstractRule {

    public DeferredDirectiveAbstractRule(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    public void checkDirective(Directive directive, List<Node> ancestors) {
        if (!directive.getName().equals(Directives.DeferDirective.getName())) {
            return;
        }

        GraphQLCompositeType parentType = getValidationContext().getParentType();
        GraphQLFieldDefinition fieldDef = getValidationContext().getFieldDef();
        if (parentType == null || fieldDef == null) {
            // some previous rule will have caught this
            return;
        }
        onDeferredDirective(directive, ancestors, parentType, fieldDef);
    }

    protected abstract void onDeferredDirective(Directive deferredDirective, List<Node> ancestors, GraphQLCompositeType parentType, GraphQLFieldDefinition fieldDef);
}
