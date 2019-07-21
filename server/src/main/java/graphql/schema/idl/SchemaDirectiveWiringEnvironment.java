package graphql.schema.idl;

import graphql.PublicApi;
import graphql.language.NamedNode;
import graphql.language.NodeParentTree;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLDirectiveContainer;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphqlElementParentTree;

import java.util.Map;

/**
 * {@link graphql.schema.idl.SchemaDirectiveWiring} is passed this object as parameters
 * when it builds out behaviour
 *
 * @param <T> the type of the object in play
 */
@PublicApi
public interface SchemaDirectiveWiringEnvironment<T extends GraphQLDirectiveContainer> {

    /**
     * @return the runtime element in play
     */
    T getElement();

    /**
     * This returns the directive that the {@link graphql.schema.idl.SchemaDirectiveWiring} was registered
     * against during calls to {@link graphql.schema.idl.RuntimeWiring.Builder#directive(String, SchemaDirectiveWiring)}
     * <p>
     * If this method of registration is not used (say because
     * {@link graphql.schema.idl.WiringFactory#providesSchemaDirectiveWiring(SchemaDirectiveWiringEnvironment)} or
     * {@link graphql.schema.idl.RuntimeWiring.Builder#directiveWiring(SchemaDirectiveWiring)} was used)
     * then this will return null.
     *
     * @return the directive that was registered under specific directive name or null if it was not
     * registered this way
     */
    GraphQLDirective getDirective();

    /**
     * @return all of the directives that are on the runtime element
     */
    Map<String, GraphQLDirective> getDirectives();

    /**
     * Returns a named directive or null
     *
     * @param directiveName the name of the directive
     *
     * @return a named directive or null
     */
    GraphQLDirective getDirective(String directiveName);

    /**
     * Returns true if the named directive is present
     *
     * @param directiveName the name of the directive
     *
     * @return true if the named directive is present
     */
    boolean containsDirective(String directiveName);

    /**
     * The node hierarchy depends on the element in question.  For example {@link graphql.language.ObjectTypeDefinition} nodes
     * have no parent, however a {@link graphql.language.Argument} might be on a {@link graphql.language.FieldDefinition}
     * which in turn might be on a {@link graphql.language.ObjectTypeDefinition} say
     *
     * @return hierarchical graphql language node information
     */
    NodeParentTree<NamedNode> getNodeParentTree();

    /**
     * The type hierarchy depends on the element in question.  For example {@link graphql.schema.GraphQLObjectType} elements
     * have no parent, however a {@link graphql.schema.GraphQLArgument} might be on a {@link graphql.schema.GraphQLFieldDefinition}
     * which in turn might be on a {@link graphql.schema.GraphQLObjectType} say
     *
     * @return hierarchical graphql type information
     */
    GraphqlElementParentTree getElementParentTree();

    /**
     * @return the type registry
     */
    TypeDefinitionRegistry getRegistry();

    /**
     * @return a mpa that can be used by implementors to hold context during the SDL build process
     */
    Map<String, Object> getBuildContext();

    /**
     * @return a builder of the current code registry builder
     */
    GraphQLCodeRegistry.Builder getCodeRegistry();

    /**
     * @return a {@link graphql.schema.GraphQLFieldsContainer} when the element is contained with a fields container
     */
    GraphQLFieldsContainer getFieldsContainer();

    /**
     * @return a {@link GraphQLFieldDefinition} when the element is as field or is contained within one
     */
    GraphQLFieldDefinition getFieldDefinition();

    /**
     * This is useful as a shortcut to get the current fields existing data fetcher
     *
     * @return a {@link graphql.schema.DataFetcher} when the element is as field or is contained within one
     *
     * @throws graphql.AssertException if there is not field in context at the time of the directive wiring callback
     */
    DataFetcher getFieldDataFetcher();

    /**
     * This is a shortcut method to set a new data fetcher in the underlying {@link graphql.schema.GraphQLCodeRegistry}
     * against the current field.
     * <p>
     * Often schema directive wiring modify behaviour by wrapping or replacing data fetchers on
     * fields.  This method is a helper to make this easier in code.
     *
     * @param newDataFetcher the new data fetcher to use for this field
     *
     * @return the environments {@link #getFieldDefinition()} to allow for a more fluent code style
     *
     * @throws graphql.AssertException if there is not field in context at the time of the directive wiring callback
     */
    GraphQLFieldDefinition setFieldDataFetcher(DataFetcher newDataFetcher);

}
