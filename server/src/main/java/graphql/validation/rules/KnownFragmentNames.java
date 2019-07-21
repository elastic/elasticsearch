package graphql.validation.rules;


import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.validation.AbstractRule;
import graphql.validation.ValidationContext;
import graphql.validation.ValidationErrorCollector;
import graphql.validation.ValidationErrorType;

public class KnownFragmentNames extends AbstractRule {

    public KnownFragmentNames(ValidationContext validationContext, ValidationErrorCollector validationErrorCollector) {
        super(validationContext, validationErrorCollector);
    }

    @Override
    public void checkFragmentSpread(FragmentSpread fragmentSpread) {
        FragmentDefinition fragmentDefinition = getValidationContext().getFragment(fragmentSpread.getName());
        if (fragmentDefinition == null) {
            String message = String.format("Undefined fragment %s", fragmentSpread.getName());
            addError(ValidationErrorType.UndefinedFragment, fragmentSpread.getSourceLocation(), message);
        }
    }
}
