package graphql.validation.rules;

import graphql.Internal;
import graphql.language.Directive;
import graphql.language.Node;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeUtil;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

import java.util.List;

@Internal
public class DeferredDirectiveOnNonNullableField extends DeferredDirectiveAbstractRule {


    public DeferredDirectiveOnNonNullableField(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    protected void onDeferredDirective(Directive deferredDirective, List<Node> ancestors, GraphQLCompositeType parentType, GraphQLFieldDefinition fieldDef) {
        GraphQLOutputType fieldDefType = fieldDef.getType();
        if (!GraphQLTypeUtil.isNullable(fieldDefType)) {
            String message = String.format("@defer directives can only be applied to nullable fields - %s.%s is non nullable", parentType.getName(), fieldDef.getName());
            addError(ValidationErrorType.DeferDirectiveOnNonNullField, deferredDirective.getSourceLocation(), message);
        }
    }
}
