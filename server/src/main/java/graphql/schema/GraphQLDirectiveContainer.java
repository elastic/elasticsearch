package graphql.schema;


import java.util.List;
import java.util.Map;

import static graphql.DirectivesUtil.directivesByName;

/**
 * Represents a graphql object that can have {@link graphql.schema.GraphQLDirective}s
 */
public interface GraphQLDirectiveContainer extends GraphQLType {

    /**
     * @return a list of directives associated with the type or field
     */
    List<GraphQLDirective> getDirectives();

    /**
     * @return a a map of directives by directive name
     */
    default Map<String, GraphQLDirective> getDirectivesByName() {
        return directivesByName(getDirectives());
    }

    /**
     * Returns a named directive
     *
     * @param directiveName the name of the directive to retrieve
     *
     * @return the directive or null if there is one one with that name
     */
    default GraphQLDirective getDirective(String directiveName) {
        return getDirectivesByName().get(directiveName);
    }

}
