package graphql.schema.idl;

import graphql.Assert;
import graphql.GraphQLError;
import graphql.PublicApi;
import graphql.language.DirectiveDefinition;
import graphql.language.EnumTypeExtensionDefinition;
import graphql.language.InputObjectTypeExtensionDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.InterfaceTypeExtensionDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.SDLDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.ScalarTypeExtensionDefinition;
import graphql.language.SchemaDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.language.UnionTypeDefinition;
import graphql.language.UnionTypeExtensionDefinition;
import graphql.schema.idl.errors.DirectiveRedefinitionError;
import graphql.schema.idl.errors.SchemaProblem;
import graphql.schema.idl.errors.SchemaRedefinitionError;
import graphql.schema.idl.errors.TypeRedefinitionError;
import graphql.util.FpKit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static graphql.Assert.assertNotNull;
import static java.util.Optional.ofNullable;

/**
 * A {@link TypeDefinitionRegistry} contains the set of type definitions that come from compiling
 * a graphql schema definition file via {@link SchemaParser#parse(String)}
 */
@PublicApi
public class TypeDefinitionRegistry {

    private final Map<String, List<ObjectTypeExtensionDefinition>> objectTypeExtensions = new LinkedHashMap<>();
    private final Map<String, List<InterfaceTypeExtensionDefinition>> interfaceTypeExtensions = new LinkedHashMap<>();
    private final Map<String, List<UnionTypeExtensionDefinition>> unionTypeExtensions = new LinkedHashMap<>();
    private final Map<String, List<EnumTypeExtensionDefinition>> enumTypeExtensions = new LinkedHashMap<>();
    private final Map<String, List<ScalarTypeExtensionDefinition>> scalarTypeExtensions = new LinkedHashMap<>();
    private final Map<String, List<InputObjectTypeExtensionDefinition>> inputObjectTypeExtensions = new LinkedHashMap<>();

    private final Map<String, TypeDefinition> types = new LinkedHashMap<>();
    private final Map<String, ScalarTypeDefinition> scalarTypes = new LinkedHashMap<>();
    private final Map<String, DirectiveDefinition> directiveDefinitions = new LinkedHashMap<>();
    private SchemaDefinition schema;

    /**
     * This will merge these type registries together and return this one
     *
     * @param typeRegistry the registry to be merged into this one
     *
     * @return this registry
     *
     * @throws SchemaProblem if there are problems merging the types such as redefinitions
     */
    public TypeDefinitionRegistry merge(TypeDefinitionRegistry typeRegistry) throws SchemaProblem {
        List<GraphQLError> errors = new ArrayList<>();

        Map<String, TypeDefinition> tempTypes = new LinkedHashMap<>();
        typeRegistry.types.values().forEach(newEntry -> {
            Optional<GraphQLError> defined = define(this.types, tempTypes, newEntry);
            defined.ifPresent(errors::add);
        });

        Map<String, DirectiveDefinition> tempDirectiveDefs = new LinkedHashMap<>();
        typeRegistry.directiveDefinitions.values().forEach(newEntry -> {
            Optional<GraphQLError> defined = define(this.directiveDefinitions, tempDirectiveDefs, newEntry);
            defined.ifPresent(errors::add);
        });

        Map<String, ScalarTypeDefinition> tempScalarTypes = new LinkedHashMap<>();
        typeRegistry.scalarTypes.values().forEach(newEntry -> define(this.scalarTypes, tempScalarTypes, newEntry).ifPresent(errors::add));

        if (typeRegistry.schema != null && this.schema != null) {
            errors.add(new SchemaRedefinitionError(this.schema, typeRegistry.schema));
        }

        if (!errors.isEmpty()) {
            throw new SchemaProblem(errors);
        }

        if (this.schema == null) {
            // ensure schema is not overwritten by merge
            this.schema = typeRegistry.schema;
        }

        // ok commit to the merge
        this.types.putAll(tempTypes);
        this.scalarTypes.putAll(tempScalarTypes);
        this.directiveDefinitions.putAll(tempDirectiveDefs);
        //
        // merge type extensions since they can be redefined by design
        typeRegistry.objectTypeExtensions.forEach((key, value) -> {
            List<ObjectTypeExtensionDefinition> currentList = this.objectTypeExtensions
                    .computeIfAbsent(key, k -> new ArrayList<>());
            currentList.addAll(value);
        });
        typeRegistry.interfaceTypeExtensions.forEach((key, value) -> {
            List<InterfaceTypeExtensionDefinition> currentList = this.interfaceTypeExtensions
                    .computeIfAbsent(key, k -> new ArrayList<>());
            currentList.addAll(value);
        });
        typeRegistry.unionTypeExtensions.forEach((key, value) -> {
            List<UnionTypeExtensionDefinition> currentList = this.unionTypeExtensions
                    .computeIfAbsent(key, k -> new ArrayList<>());
            currentList.addAll(value);
        });
        typeRegistry.enumTypeExtensions.forEach((key, value) -> {
            List<EnumTypeExtensionDefinition> currentList = this.enumTypeExtensions
                    .computeIfAbsent(key, k -> new ArrayList<>());
            currentList.addAll(value);
        });
        typeRegistry.scalarTypeExtensions.forEach((key, value) -> {
            List<ScalarTypeExtensionDefinition> currentList = this.scalarTypeExtensions
                    .computeIfAbsent(key, k -> new ArrayList<>());
            currentList.addAll(value);
        });
        typeRegistry.inputObjectTypeExtensions.forEach((key, value) -> {
            List<InputObjectTypeExtensionDefinition> currentList = this.inputObjectTypeExtensions
                    .computeIfAbsent(key, k -> new ArrayList<>());
            currentList.addAll(value);
        });

        return this;
    }

    /**
     * Adds a a collections of definitions to the registry
     *
     * @param definitions the definitions to add
     *
     * @return an optional error for the first problem, typically type redefinition
     */
    public Optional<GraphQLError> addAll(Collection<SDLDefinition> definitions) {
        for (SDLDefinition definition : definitions) {
            Optional<GraphQLError> error = add(definition);
            if (error.isPresent()) {
                return error;
            }
        }
        return Optional.empty();
    }

    /**
     * Adds a definition to the registry
     *
     * @param definition the definition to add
     *
     * @return an optional error
     */
    public Optional<GraphQLError> add(SDLDefinition definition) {
        // extensions
        if (definition instanceof ObjectTypeExtensionDefinition) {
            ObjectTypeExtensionDefinition newEntry = (ObjectTypeExtensionDefinition) definition;
            return defineExt(objectTypeExtensions, newEntry, ObjectTypeExtensionDefinition::getName);
        } else if (definition instanceof InterfaceTypeExtensionDefinition) {
            InterfaceTypeExtensionDefinition newEntry = (InterfaceTypeExtensionDefinition) definition;
            return defineExt(interfaceTypeExtensions, newEntry, InterfaceTypeExtensionDefinition::getName);
        } else if (definition instanceof UnionTypeExtensionDefinition) {
            UnionTypeExtensionDefinition newEntry = (UnionTypeExtensionDefinition) definition;
            return defineExt(unionTypeExtensions, newEntry, UnionTypeExtensionDefinition::getName);
        } else if (definition instanceof EnumTypeExtensionDefinition) {
            EnumTypeExtensionDefinition newEntry = (EnumTypeExtensionDefinition) definition;
            return defineExt(enumTypeExtensions, newEntry, EnumTypeExtensionDefinition::getName);
        } else if (definition instanceof ScalarTypeExtensionDefinition) {
            ScalarTypeExtensionDefinition newEntry = (ScalarTypeExtensionDefinition) definition;
            return defineExt(scalarTypeExtensions, newEntry, ScalarTypeExtensionDefinition::getName);
        } else if (definition instanceof InputObjectTypeExtensionDefinition) {
            InputObjectTypeExtensionDefinition newEntry = (InputObjectTypeExtensionDefinition) definition;
            return defineExt(inputObjectTypeExtensions, newEntry, InputObjectTypeExtensionDefinition::getName);
            //
            // normal
        } else if (definition instanceof ScalarTypeDefinition) {
            ScalarTypeDefinition newEntry = (ScalarTypeDefinition) definition;
            return define(scalarTypes, scalarTypes, newEntry);
        } else if (definition instanceof TypeDefinition) {
            TypeDefinition newEntry = (TypeDefinition) definition;
            return define(types, types, newEntry);
        } else if (definition instanceof DirectiveDefinition) {
            DirectiveDefinition newEntry = (DirectiveDefinition) definition;
            return define(directiveDefinitions, directiveDefinitions, newEntry);
        } else if (definition instanceof SchemaDefinition) {
            SchemaDefinition newSchema = (SchemaDefinition) definition;
            if (schema != null) {
                return Optional.of(new SchemaRedefinitionError(this.schema, newSchema));
            } else {
                schema = newSchema;
            }
        } else {
            return Assert.assertShouldNeverHappen();
        }
        return Optional.empty();
    }

    public void remove(SDLDefinition definition) {
        assertNotNull(definition, "definition to remove can't be null");
        if (definition instanceof ObjectTypeExtensionDefinition) {
            removeFromList(objectTypeExtensions, (TypeDefinition) definition);
        } else if (definition instanceof InterfaceTypeExtensionDefinition) {
            removeFromList(interfaceTypeExtensions, (TypeDefinition) definition);
        } else if (definition instanceof UnionTypeExtensionDefinition) {
            removeFromList(unionTypeExtensions, (TypeDefinition) definition);
        } else if (definition instanceof EnumTypeExtensionDefinition) {
            removeFromList(enumTypeExtensions, (TypeDefinition) definition);
        } else if (definition instanceof ScalarTypeExtensionDefinition) {
            removeFromList(scalarTypeExtensions, (TypeDefinition) definition);
        } else if (definition instanceof InputObjectTypeExtensionDefinition) {
            removeFromList(inputObjectTypeExtensions, (TypeDefinition) definition);
        } else if (definition instanceof ScalarTypeDefinition) {
            scalarTypes.remove(((ScalarTypeDefinition) definition).getName());
        } else if (definition instanceof TypeDefinition) {
            types.remove(((TypeDefinition) definition).getName());
        } else if (definition instanceof DirectiveDefinition) {
            directiveDefinitions.remove(((DirectiveDefinition) definition).getName());
        } else if (definition instanceof SchemaDefinition) {
            schema = null;
        } else {
            Assert.assertShouldNeverHappen();
        }
    }

    private void removeFromList(Map source, TypeDefinition value) {
        List<TypeDefinition> list = (List<TypeDefinition>) source.get(value.getName());
        if (list == null) {
            return;
        }
        list.remove(value);
    }


    private <T extends TypeDefinition> Optional<GraphQLError> define(Map<String, T> source, Map<String, T> target, T newEntry) {
        String name = newEntry.getName();

        T olderEntry = source.get(name);
        if (olderEntry != null) {
            return Optional.of(handleReDefinition(olderEntry, newEntry));
        } else {
            target.put(name, newEntry);
        }
        return Optional.empty();
    }

    private <T extends DirectiveDefinition> Optional<GraphQLError> define(Map<String, T> source, Map<String, T> target, T newEntry) {
        String name = newEntry.getName();

        T olderEntry = source.get(name);
        if (olderEntry != null) {
            return Optional.of(handleReDefinition(olderEntry, newEntry));
        } else {
            target.put(name, newEntry);
        }
        return Optional.empty();
    }

    private <T> Optional<GraphQLError> defineExt(Map<String, List<T>> typeExtensions, T newEntry, Function<T, String> namerFunc) {
        List<T> currentList = typeExtensions.computeIfAbsent(namerFunc.apply(newEntry), k -> new ArrayList<>());
        currentList.add(newEntry);
        return Optional.empty();
    }

    public Map<String, TypeDefinition> types() {
        return new LinkedHashMap<>(types);
    }

    public Map<String, ScalarTypeDefinition> scalars() {
        LinkedHashMap<String, ScalarTypeDefinition> scalars = new LinkedHashMap<>(ScalarInfo.STANDARD_SCALAR_DEFINITIONS);
        scalars.putAll(scalarTypes);
        return scalars;
    }

    public Map<String, List<ObjectTypeExtensionDefinition>> objectTypeExtensions() {
        return new LinkedHashMap<>(objectTypeExtensions);
    }

    public Map<String, List<InterfaceTypeExtensionDefinition>> interfaceTypeExtensions() {
        return new LinkedHashMap<>(interfaceTypeExtensions);
    }

    public Map<String, List<UnionTypeExtensionDefinition>> unionTypeExtensions() {
        return new LinkedHashMap<>(unionTypeExtensions);
    }

    public Map<String, List<EnumTypeExtensionDefinition>> enumTypeExtensions() {
        return new LinkedHashMap<>(enumTypeExtensions);
    }

    public Map<String, List<ScalarTypeExtensionDefinition>> scalarTypeExtensions() {
        return new LinkedHashMap<>(scalarTypeExtensions);
    }

    public Map<String, List<InputObjectTypeExtensionDefinition>> inputObjectTypeExtensions() {
        return new LinkedHashMap<>(inputObjectTypeExtensions);
    }

    public Optional<SchemaDefinition> schemaDefinition() {
        return ofNullable(schema);
    }

    private GraphQLError handleReDefinition(TypeDefinition oldEntry, TypeDefinition newEntry) {
        return new TypeRedefinitionError(newEntry, oldEntry);
    }

    private GraphQLError handleReDefinition(DirectiveDefinition oldEntry, DirectiveDefinition newEntry) {
        return new DirectiveRedefinitionError(newEntry, oldEntry);
    }

    public Optional<DirectiveDefinition> getDirectiveDefinition(String directiveName) {
        return Optional.ofNullable(directiveDefinitions.get(directiveName));
    }

    public Map<String, DirectiveDefinition> getDirectiveDefinitions() {
        return new LinkedHashMap<>(directiveDefinitions);
    }

    public boolean hasType(TypeName typeName) {
        String name = typeName.getName();
        return types.containsKey(name) || ScalarInfo.STANDARD_SCALAR_DEFINITIONS.containsKey(name) || scalarTypes.containsKey(name) || objectTypeExtensions.containsKey(name);
    }

    public Optional<TypeDefinition> getType(Type type) {
        String typeName = TypeInfo.typeInfo(type).getName();
        return getType(typeName);
    }

    public <T extends TypeDefinition> Optional<T> getType(Type type, Class<T> ofType) {
        String typeName = TypeInfo.typeInfo(type).getName();
        return getType(typeName, ofType);
    }

    public Optional<TypeDefinition> getType(String typeName) {
        TypeDefinition<?> typeDefinition = types.get(typeName);
        if (typeDefinition != null) {
            return Optional.of(typeDefinition);
        }
        typeDefinition = scalars().get(typeName);
        if (typeDefinition != null) {
            return Optional.of(typeDefinition);
        }
        return Optional.empty();
    }

    public <T extends TypeDefinition> Optional<T> getType(String typeName, Class<T> ofType) {
        Optional<TypeDefinition> type = getType(typeName);
        if (type.isPresent()) {
            TypeDefinition typeDefinition = type.get();
            if (typeDefinition.getClass().equals(ofType)) {
                //noinspection unchecked
                return Optional.of((T) typeDefinition);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns true if the specified type exists in the registry and is an abstract (Interface or Union) type
     *
     * @param type the type to check
     *
     * @return true if its abstract
     */
    public boolean isInterfaceOrUnion(Type type) {
        Optional<TypeDefinition> typeDefinition = getType(type);
        if (typeDefinition.isPresent()) {
            TypeDefinition definition = typeDefinition.get();
            return definition instanceof UnionTypeDefinition || definition instanceof InterfaceTypeDefinition;
        }
        return false;
    }

    /**
     * Returns true if the specified type exists in the registry and is an object type
     *
     * @param type the type to check
     *
     * @return true if its an object type
     */
    public boolean isObjectType(Type type) {
        return getType(type, ObjectTypeDefinition.class).isPresent();
    }

    /**
     * Returns a list of types in the registry of that specified class
     *
     * @param targetClass the class to search for
     * @param <T>         must extend TypeDefinition
     *
     * @return a list of types of the target class
     */
    public <T extends TypeDefinition> List<T> getTypes(Class<T> targetClass) {
        return types.values().stream()
                .filter(targetClass::isInstance)
                .map(targetClass::cast)
                .collect(Collectors.toList());
    }

    /**
     * Returns a map of types in the registry of that specified class keyed by name
     *
     * @param targetClass the class to search for
     * @param <T>         must extend TypeDefinition
     *
     * @return a map of types
     */
    public <T extends TypeDefinition> Map<String, T> getTypesMap(Class<T> targetClass) {
        List<T> list = getTypes(targetClass);
        return FpKit.getByName(list, TypeDefinition::getName, FpKit.mergeFirst());
    }

    /**
     * Returns the list of object types that implement the given interface type
     *
     * @param targetInterface the target to search for
     *
     * @return the list of object types that implement the given interface type
     */
    public List<ObjectTypeDefinition> getImplementationsOf(InterfaceTypeDefinition targetInterface) {
        List<ObjectTypeDefinition> objectTypeDefinitions = getTypes(ObjectTypeDefinition.class);
        return objectTypeDefinitions.stream().filter(objectTypeDefinition -> {
            List<Type> implementsList = objectTypeDefinition.getImplements();
            for (Type iFace : implementsList) {
                Optional<InterfaceTypeDefinition> interfaceTypeDef = getType(iFace, InterfaceTypeDefinition.class);
                if (interfaceTypeDef.isPresent()) {
                    boolean equals = interfaceTypeDef.get().getName().equals(targetInterface.getName());
                    if (equals) {
                        return true;
                    }
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    /**
     * Returns true of the abstract type is in implemented by the object type
     *
     * @param abstractType       the abstract type to check (interface or union)
     * @param possibleObjectType the object type to check
     *
     * @return true if the object type implements the abstract type
     */
    @SuppressWarnings("ConstantConditions")
    public boolean isPossibleType(Type abstractType, Type possibleObjectType) {
        if (!isInterfaceOrUnion(abstractType)) {
            return false;
        }
        if (!isObjectType(possibleObjectType)) {
            return false;
        }
        ObjectTypeDefinition targetObjectTypeDef = getType(possibleObjectType, ObjectTypeDefinition.class).get();
        TypeDefinition abstractTypeDef = getType(abstractType).get();
        if (abstractTypeDef instanceof UnionTypeDefinition) {
            List<Type> memberTypes = ((UnionTypeDefinition) abstractTypeDef).getMemberTypes();
            for (Type memberType : memberTypes) {
                Optional<ObjectTypeDefinition> checkType = getType(memberType, ObjectTypeDefinition.class);
                if (checkType.isPresent()) {
                    if (checkType.get().getName().equals(targetObjectTypeDef.getName())) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            InterfaceTypeDefinition iFace = (InterfaceTypeDefinition) abstractTypeDef;
            List<ObjectTypeDefinition> objectTypeDefinitions = getImplementationsOf(iFace);
            return objectTypeDefinitions.stream()
                    .anyMatch(od -> od.getName().equals(targetObjectTypeDef.getName()));
        }
    }

    /**
     * Returns true if the maybe type is either equal or a subset of the second super type (covariant).
     *
     * @param maybeSubType the type to check
     * @param superType    the equality checked type
     *
     * @return true if maybeSubType is covariant or equal to superType
     */
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean isSubTypeOf(Type maybeSubType, Type superType) {
        TypeInfo maybeSubTypeInfo = TypeInfo.typeInfo(maybeSubType);
        TypeInfo superTypeInfo = TypeInfo.typeInfo(superType);
        // Equivalent type is a valid subtype
        if (maybeSubTypeInfo.equals(superTypeInfo)) {
            return true;
        }


        // If superType is non-null, maybeSubType must also be non-null.
        if (superTypeInfo.isNonNull()) {
            if (maybeSubTypeInfo.isNonNull()) {
                return isSubTypeOf(maybeSubTypeInfo.unwrapOneType(), superTypeInfo.unwrapOneType());
            }
            return false;
        }
        if (maybeSubTypeInfo.isNonNull()) {
            // If superType is nullable, maybeSubType may be non-null or nullable.
            return isSubTypeOf(maybeSubTypeInfo.unwrapOneType(), superType);
        }

        // If superType type is a list, maybeSubType type must also be a list.
        if (superTypeInfo.isList()) {
            if (maybeSubTypeInfo.isList()) {
                return isSubTypeOf(maybeSubTypeInfo.unwrapOneType(), superTypeInfo.unwrapOneType());
            }
            return false;
        }
        if (maybeSubTypeInfo.isList()) {
            // If superType is not a list, maybeSubType must also be not a list.
            return false;
        }

        // If superType type is an abstract type, maybeSubType type may be a currently
        // possible object type.
        if (isInterfaceOrUnion(superType) &&
                isObjectType(maybeSubType) &&
                isPossibleType(superType, maybeSubType)) {
            return true;
        }

        // Otherwise, the child type is not a valid subtype of the parent type.
        return false;
    }

}
