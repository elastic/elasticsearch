package graphql.analysis;

import graphql.Internal;
import graphql.language.SelectionSetContainer;
import graphql.schema.GraphQLCompositeType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeUtil;

/**
 * QueryTraverser helper class that maintains traversal context as
 * the query traversal algorithm traverses down the Selection AST
 */
@Internal
class QueryTraversalContext {

    // never used for scalars/enums, always a possibly wrapped composite type
    private final GraphQLOutputType outputType;
    private final QueryVisitorFieldEnvironment environment;
    private final SelectionSetContainer selectionSetContainer;

    QueryTraversalContext(GraphQLOutputType outputType,
                          QueryVisitorFieldEnvironment environment,
                          SelectionSetContainer selectionSetContainer) {
        this.outputType = outputType;
        this.environment = environment;
        this.selectionSetContainer = selectionSetContainer;
    }

    public GraphQLOutputType getOutputType() {
        return outputType;
    }

    public GraphQLCompositeType getUnwrappedOutputType() {
        return (GraphQLCompositeType) GraphQLTypeUtil.unwrapAll(outputType);
    }


    public QueryVisitorFieldEnvironment getEnvironment() {
        return environment;
    }

    public SelectionSetContainer getSelectionSetContainer() {

        return selectionSetContainer;
    }
}
