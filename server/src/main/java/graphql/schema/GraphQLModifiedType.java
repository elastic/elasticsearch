package graphql.schema;


/**
 * A modified type wraps another graphql type and modifies it behavior
 *
 * @see graphql.schema.GraphQLNonNull
 * @see graphql.schema.GraphQLList
 */
public interface GraphQLModifiedType extends GraphQLType {

    GraphQLType getWrappedType();
}
