package graphql.analysis;

import graphql.PublicApi;
import graphql.language.Field;
import graphql.language.Node;
import graphql.language.SelectionSetContainer;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLOutputType;
import graphql.util.TraverserContext;

import java.util.Map;

@PublicApi
public interface QueryVisitorFieldEnvironment {

    /**
     * @return true if the current field is __typename
     */
    boolean isTypeNameIntrospectionField();

    /**
     * @return the current Field
     */
    Field getField();

    GraphQLFieldDefinition getFieldDefinition();

    /**
     * @return the parent output type of the current field.
     */
    GraphQLOutputType getParentType();

    /**
     * @return the unmodified fields container fot the current type. This is the unwrapped version of {@link #getParentType()}
     * It is either {@link graphql.schema.GraphQLObjectType} or {@link graphql.schema.GraphQLInterfaceType}. because these
     * are the only {@link GraphQLFieldsContainer}
     *
     * @throws IllegalStateException if the current field is __typename see {@link #isTypeNameIntrospectionField()}
     */
    GraphQLFieldsContainer getFieldsContainer();

    QueryVisitorFieldEnvironment getParentEnvironment();

    Map<String, Object> getArguments();

    SelectionSetContainer getSelectionSetContainer();

    TraverserContext<Node> getTraverserContext();
}
