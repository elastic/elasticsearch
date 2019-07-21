package graphql.language;


import graphql.PublicApi;

import java.util.List;
import java.util.Map;

import static graphql.language.NodeUtil.directivesByName;

/**
 * Represents a language node that can contain Directives.
 */
@PublicApi
public interface DirectivesContainer<T extends DirectivesContainer> extends Node<T> {

    /**
     * @return a list of directives associated with this Node
     */
    List<Directive> getDirectives();

    /**
     * @return a a map of directives by directive name
     */
    default Map<String, Directive> getDirectivesByName() {
        return directivesByName(getDirectives());
    }

    /**
     * Returns a directive with the provided name
     *
     * @param directiveName the name of the directive to retrieve
     *
     * @return the directive or null if there is one one with that name
     */
    default Directive getDirective(String directiveName) {
        return getDirectivesByName().get(directiveName);
    }
}
