package graphql.execution;

import graphql.Internal;
import graphql.VisibleForTesting;
import graphql.language.Directive;

import java.util.List;
import java.util.Map;

import static graphql.Directives.IncludeDirective;
import static graphql.Directives.SkipDirective;
import static graphql.language.NodeUtil.directivesByName;


@Internal
public class ConditionalNodes {


    @VisibleForTesting
    ValuesResolver valuesResolver = new ValuesResolver();

    public boolean shouldInclude(Map<String, Object> variables, List<Directive> directives) {
        boolean skip = getDirectiveResult(variables, directives, SkipDirective.getName(), false);
        boolean include = getDirectiveResult(variables, directives, IncludeDirective.getName(), true);
        return !skip && include;
    }

    private Directive getDirectiveByName(List<Directive> directives, String name) {
        if (directives.isEmpty()) {
            return null;
        }
        return directivesByName(directives).get(name);
    }

    private boolean getDirectiveResult(Map<String, Object> variables, List<Directive> directives, String directiveName, boolean defaultValue) {
        Directive directive = getDirectiveByName(directives, directiveName);
        if (directive != null) {
            Map<String, Object> argumentValues = valuesResolver.getArgumentValues(SkipDirective.getArguments(), directive.getArguments(), variables);
            return (Boolean) argumentValues.get("if");
        }

        return defaultValue;
    }

}
