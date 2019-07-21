package graphql.language;

import graphql.PublicApi;

import java.util.List;

@PublicApi
public interface NodeDirectivesBuilder extends NodeBuilder {

    NodeDirectivesBuilder directives(List<Directive> directives);

}
