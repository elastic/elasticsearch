package graphql.schema.idl;

import graphql.Internal;
import graphql.language.AstPrinter;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.GraphQLType;

import java.util.Objects;
import java.util.Stack;

import static graphql.Assert.assertNotNull;
import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;

/**
 * This helper gives you access to the type info given a type definition
 */
@Internal
public class TypeInfo {

    public static TypeInfo typeInfo(Type type) {
        return new TypeInfo(type);
    }

    private final Type rawType;
    private final TypeName typeName;
    private final Stack<Class<?>> decoration = new Stack<>();

    private TypeInfo(Type type) {
        this.rawType = assertNotNull(type, "type must not be null");
        while (!(type instanceof TypeName)) {
            if (type instanceof NonNullType) {
                decoration.push(NonNullType.class);
                type = ((NonNullType) type).getType();
            }
            if (type instanceof ListType) {
                decoration.push(ListType.class);
                type = ((ListType) type).getType();
            }
        }
        this.typeName = (TypeName) type;
    }

    public Type getRawType() {
        return rawType;
    }

    public TypeName getTypeName() {
        return typeName;
    }

    public String getName() {
        return typeName.getName();
    }

    public boolean isList() {
        return rawType instanceof ListType;
    }

    public boolean isNonNull() {
        return rawType instanceof NonNullType;
    }

    public boolean isPlain() {
        return !isList() && !isNonNull();
    }

    /**
     * This will rename the type with the specified new name but will preserve the wrapping that was present
     *
     * @param newName the new name of the type
     *
     * @return a new type info rebuilt with the new name
     */
    public TypeInfo renameAs(String newName) {

        Type out = TypeName.newTypeName(newName).build();

        Stack<Class<?>> wrappingStack = new Stack<>();
        wrappingStack.addAll(this.decoration);
        while (!wrappingStack.isEmpty()) {
            Class<?> clazz = wrappingStack.pop();
            if (clazz.equals(NonNullType.class)) {
                out = NonNullType.newNonNullType(out).build();
            }
            if (clazz.equals(ListType.class)) {
                out = ListType.newListType(out).build();
            }
        }
        return typeInfo(out);
    }

    /**
     * This will decorate a graphql type with the original hierarchy of non null and list'ness
     * it originally contained in its definition type
     *
     * @param objectType this should be a graphql type that was originally built from this raw type
     * @param <T>        the type
     *
     * @return the decorated type
     */
    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <T extends GraphQLType> T decorate(GraphQLType objectType) {

        GraphQLType out = objectType;
        Stack<Class<?>> wrappingStack = new Stack<>();
        wrappingStack.addAll(this.decoration);
        while (!wrappingStack.isEmpty()) {
            Class<?> clazz = wrappingStack.pop();
            if (clazz.equals(NonNullType.class)) {
                out = nonNull(out);
            }
            if (clazz.equals(ListType.class)) {
                out = list(out);
            }
        }
        // we handle both input and output graphql types
        //noinspection unchecked
        return (T) out;
    }

    public static String getAstDesc(Type type) {
        return AstPrinter.printAst(type);
    }

    public TypeInfo unwrapOne() {
        if (rawType instanceof NonNullType) {
            return typeInfo(((NonNullType) rawType).getType());
        }
        if (rawType instanceof ListType) {
            return typeInfo(((ListType) rawType).getType());
        }
        return this;
    }

    public Type unwrapOneType() {
        return unwrapOne().getRawType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypeInfo typeInfo = (TypeInfo) o;
        return isNonNull() == typeInfo.isNonNull() &&
                isList() == typeInfo.isList() &&
                Objects.equals(typeName.getName(), typeInfo.typeName.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName.getName(), isNonNull(), isList());
    }


    @Override
    public String toString() {
        return "TypeInfo{" +
                getAstDesc(rawType) +
                '}';
    }
}

