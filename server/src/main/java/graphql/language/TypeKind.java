package graphql.language;

import graphql.Assert;
import graphql.PublicApi;

/**
 * And enumeration of the the kind of things that can be in a graphql type system
 */
@PublicApi
public enum TypeKind {

    Operation, Object, Interface, Union, Enum, Scalar, InputObject;

    public static TypeKind getTypeKind(TypeDefinition def) {
        if (def instanceof ObjectTypeDefinition) {
            return Object;
        }
        if (def instanceof InterfaceTypeDefinition) {
            return Interface;
        }
        if (def instanceof UnionTypeDefinition) {
            return Union;
        }
        if (def instanceof ScalarTypeDefinition) {
            return Scalar;
        }
        if (def instanceof EnumTypeDefinition) {
            return Enum;
        }
        if (def instanceof InputObjectTypeDefinition) {
            return InputObject;
        }
        return Assert.assertShouldNeverHappen();
    }
}
