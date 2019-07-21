package graphql.schema.idl;

import graphql.PublicApi;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLUnionType;

/**
 * A SchemaDirectiveWiring is responsible for enhancing a runtime element based on directives placed on that
 * element in the Schema Definition Language (SDL).
 * <p>
 * It can enhance the graphql runtime element and add new behaviour for example by changing
 * the fields {@link graphql.schema.DataFetcher}
 * <p>
 * The SchemaDirectiveWiring objects are called in a specific order based on registration:
 * <ol>
 * <li>{@link graphql.schema.idl.RuntimeWiring.Builder#directive(String, SchemaDirectiveWiring)} which work against a specific named directive are called first</li>
 * <li>{@link graphql.schema.idl.RuntimeWiring.Builder#directiveWiring(SchemaDirectiveWiring)} which work against all directives are called next</li>
 * <li>{@link graphql.schema.idl.WiringFactory#providesSchemaDirectiveWiring(SchemaDirectiveWiringEnvironment)} which work against all directives are called last</li>
 * </ol>
 * <p>
 */
@PublicApi
public interface SchemaDirectiveWiring {

    /**
     * This is called when an object is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL element
     * <p>
     * The {@link #onArgument(SchemaDirectiveWiringEnvironment)} and {@link #onField(SchemaDirectiveWiringEnvironment)} callbacks will have been
     * invoked for this element beforehand
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLObjectType onObject(SchemaDirectiveWiringEnvironment<GraphQLObjectType> environment) {
        return environment.getElement();
    }

    /**
     * This is called when a field is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL element
     * <p>
     * The {@link #onArgument(SchemaDirectiveWiringEnvironment)} callbacks will have been
     * invoked for this element beforehand
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLFieldDefinition onField(SchemaDirectiveWiringEnvironment<GraphQLFieldDefinition> environment) {
        return environment.getElement();
    }

    /**
     * This is called when an argument is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL element
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLArgument onArgument(SchemaDirectiveWiringEnvironment<GraphQLArgument> environment) {
        return environment.getElement();
    }

    /**
     * This is called when an interface is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL element
     * <p>
     * The {@link #onArgument(SchemaDirectiveWiringEnvironment)} and {@link #onField(SchemaDirectiveWiringEnvironment)} callbacks will have been
     * invoked for this element beforehand
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLInterfaceType onInterface(SchemaDirectiveWiringEnvironment<GraphQLInterfaceType> environment) {
        return environment.getElement();
    }

    /**
     * This is called when a union is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL element
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLUnionType onUnion(SchemaDirectiveWiringEnvironment<GraphQLUnionType> environment) {
        return environment.getElement();
    }

    /**
     * This is called when an enum is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL element
     * <p>
     * The {@link #onEnumValue(SchemaDirectiveWiringEnvironment)} callbacks will have been invoked for this element beforehand
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLEnumType onEnum(SchemaDirectiveWiringEnvironment<GraphQLEnumType> environment) {
        return environment.getElement();
    }

    /**
     * This is called when an enum value is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL element
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLEnumValueDefinition onEnumValue(SchemaDirectiveWiringEnvironment<GraphQLEnumValueDefinition> environment) {
        return environment.getElement();
    }

    /**
     * This is called when a custom scalar is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL  element
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLScalarType onScalar(SchemaDirectiveWiringEnvironment<GraphQLScalarType> environment) {
        return environment.getElement();
    }

    /**
     * This is called when an input object is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL  element
     * <p>
     * The {@link #onInputObjectField(SchemaDirectiveWiringEnvironment)}callbacks will have been invoked for this element beforehand
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLInputObjectType onInputObjectType(SchemaDirectiveWiringEnvironment<GraphQLInputObjectType> environment) {
        return environment.getElement();
    }

    /**
     * This is called when an input object field is encountered, which gives the schema directive a chance to modify the shape and behaviour
     * of that DSL  element
     *
     * @param environment the wiring element
     *
     * @return a non null element based on the original one
     */
    default GraphQLInputObjectField onInputObjectField(SchemaDirectiveWiringEnvironment<GraphQLInputObjectField> environment) {
        return environment.getElement();
    }
}
