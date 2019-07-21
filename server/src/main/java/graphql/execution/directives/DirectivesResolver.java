package graphql.execution.directives;

import graphql.Internal;
import graphql.execution.ValuesResolver;
import graphql.language.Directive;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLSchema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This turns AST directives into runtime directives with resolved types and so on
 */
@Internal
public class DirectivesResolver {

    private final ValuesResolver valuesResolver = new ValuesResolver();

    public DirectivesResolver() {
    }

    public Map<String, GraphQLDirective> resolveDirectives(List<Directive> directives, GraphQLSchema schema, Map<String, Object> variables) {
        GraphQLCodeRegistry codeRegistry = schema.getCodeRegistry();
        Map<String, GraphQLDirective> directiveMap = new LinkedHashMap<>();
        directives.forEach(directive -> {
            GraphQLDirective protoType = schema.getDirective(directive.getName());
            if (protoType != null) {
                GraphQLDirective newDirective = protoType.transform(builder -> buildArguments(builder, codeRegistry, protoType, directive, variables));
                directiveMap.put(newDirective.getName(), newDirective);
            }
        });
        return directiveMap;
    }

    private void buildArguments(GraphQLDirective.Builder directiveBuilder, GraphQLCodeRegistry codeRegistry, GraphQLDirective protoType, Directive fieldDirective, Map<String, Object> variables) {
        Map<String, Object> argumentValues = valuesResolver.getArgumentValues(codeRegistry, protoType.getArguments(), fieldDirective.getArguments(), variables);
        directiveBuilder.clearArguments();
        protoType.getArguments().forEach(protoArg -> {
            if (argumentValues.containsKey(protoArg.getName())) {
                Object argValue = argumentValues.get(protoArg.getName());
                GraphQLArgument newArgument = protoArg.transform(argBuilder -> argBuilder.value(argValue));
                directiveBuilder.argument(newArgument);
            } else {
                // this means they can ask for the argument default value because the arugment on the directive
                // object is present - but null
                GraphQLArgument newArgument = protoArg.transform(argBuilder -> argBuilder.value(null));
                directiveBuilder.argument(newArgument);
            }
        });
    }
}