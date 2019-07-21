package graphql.schema.idl;

import graphql.GraphQLError;
import graphql.Internal;
import graphql.introspection.Introspection;
import graphql.language.Argument;
import graphql.language.AstPrinter;
import graphql.language.Directive;
import graphql.language.DirectiveDefinition;
import graphql.language.EnumTypeDefinition;
import graphql.language.EnumValueDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.Node;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.OperationTypeDefinition;
import graphql.language.SchemaDefinition;
import graphql.language.StringValue;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.language.UnionTypeDefinition;
import graphql.schema.idl.errors.DirectiveIllegalLocationError;
import graphql.schema.idl.errors.InterfaceFieldArgumentRedefinitionError;
import graphql.schema.idl.errors.InterfaceFieldRedefinitionError;
import graphql.schema.idl.errors.InvalidDeprecationDirectiveError;
import graphql.schema.idl.errors.MissingInterfaceFieldArgumentsError;
import graphql.schema.idl.errors.MissingInterfaceFieldError;
import graphql.schema.idl.errors.MissingInterfaceTypeError;
import graphql.schema.idl.errors.MissingScalarImplementationError;
import graphql.schema.idl.errors.MissingTypeError;
import graphql.schema.idl.errors.MissingTypeResolverError;
import graphql.schema.idl.errors.NonUniqueArgumentError;
import graphql.schema.idl.errors.NonUniqueDirectiveError;
import graphql.schema.idl.errors.NonUniqueNameError;
import graphql.schema.idl.errors.OperationTypesMustBeObjects;
import graphql.schema.idl.errors.QueryOperationMissingError;
import graphql.schema.idl.errors.SchemaMissingError;
import graphql.schema.idl.errors.SchemaProblem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This helps pre check the state of the type system to ensure it can be made into an executable schema.
 * <p>
 * It looks for missing types and ensure certain invariants are true before a schema can be made.
 */
@Internal
public class SchemaTypeChecker {

    public List<GraphQLError> checkTypeRegistry(TypeDefinitionRegistry typeRegistry, RuntimeWiring wiring, boolean enforceSchemaDirectives) throws SchemaProblem {
        List<GraphQLError> errors = new ArrayList<>();
        checkForMissingTypes(errors, typeRegistry);

        SchemaTypeExtensionsChecker typeExtensionsChecker = new SchemaTypeExtensionsChecker();

        typeExtensionsChecker.checkTypeExtensions(errors, typeRegistry);

        checkInterfacesAreImplemented(errors, typeRegistry);

        checkSchemaInvariants(errors, typeRegistry);

        checkScalarImplementationsArePresent(errors, typeRegistry, wiring);
        checkTypeResolversArePresent(errors, typeRegistry, wiring);

        checkFieldsAreSensible(errors, typeRegistry);

        //check directive definitions before checking directive usages
        checkDirectiveDefinitions(typeRegistry, errors);

        if (enforceSchemaDirectives) {
            SchemaTypeDirectivesChecker directivesChecker = new SchemaTypeDirectivesChecker(typeRegistry, wiring);
            directivesChecker.checkTypeDirectives(errors);
        }

        return errors;
    }

    private void checkSchemaInvariants(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry) {
        /*
            https://github.com/facebook/graphql/pull/90/files#diff-fe406b08746616e2f5f00909488cce66R1000

            GraphQL type system definitions can omit the schema definition when the query
            and mutation root types are named `Query` and `Mutation`, respectively.
         */
        // schema
        if (!typeRegistry.schemaDefinition().isPresent()) {
            if (!typeRegistry.getType("Query").isPresent()) {
                errors.add(new SchemaMissingError());
            }
        } else {
            SchemaDefinition schemaDefinition = typeRegistry.schemaDefinition().get();
            List<OperationTypeDefinition> operationTypeDefinitions = schemaDefinition.getOperationTypeDefinitions();

            operationTypeDefinitions
                    .forEach(checkOperationTypesExist(typeRegistry, errors));

            operationTypeDefinitions
                    .forEach(checkOperationTypesAreObjects(typeRegistry, errors));

            // ensure we have a "query" one
            Optional<OperationTypeDefinition> query = operationTypeDefinitions.stream().filter(op -> "query".equals(op.getName())).findFirst();
            if (!query.isPresent()) {
                errors.add(new QueryOperationMissingError());
            }

        }
    }


    private void checkForMissingTypes(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry) {
        // type extensions
        List<ObjectTypeExtensionDefinition> typeExtensions = typeRegistry.objectTypeExtensions().values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        typeExtensions.forEach(typeExtension -> {

            List<Type> implementsTypes = typeExtension.getImplements();
            implementsTypes.forEach(checkInterfaceTypeExists(typeRegistry, errors, typeExtension));

            checkFieldTypesPresent(typeRegistry, errors, typeExtension, typeExtension.getFieldDefinitions());

        });


        Map<String, TypeDefinition> typesMap = typeRegistry.types();

        // objects
        List<ObjectTypeDefinition> objectTypes = filterTo(typesMap, ObjectTypeDefinition.class);
        objectTypes.forEach(objectType -> {

            List<Type> implementsTypes = objectType.getImplements();
            implementsTypes.forEach(checkInterfaceTypeExists(typeRegistry, errors, objectType));

            checkFieldTypesPresent(typeRegistry, errors, objectType, objectType.getFieldDefinitions());

        });

        // interfaces
        List<InterfaceTypeDefinition> interfaceTypes = filterTo(typesMap, InterfaceTypeDefinition.class);
        interfaceTypes.forEach(interfaceType -> {
            List<FieldDefinition> fields = interfaceType.getFieldDefinitions();

            checkFieldTypesPresent(typeRegistry, errors, interfaceType, fields);

        });

        // union types
        List<UnionTypeDefinition> unionTypes = filterTo(typesMap, UnionTypeDefinition.class);
        unionTypes.forEach(unionType -> {
            List<Type> memberTypes = unionType.getMemberTypes();
            memberTypes.forEach(checkTypeExists("union member", typeRegistry, errors, unionType));

        });


        // input types
        List<InputObjectTypeDefinition> inputTypes = filterTo(typesMap, InputObjectTypeDefinition.class);
        inputTypes.forEach(inputType -> {
            List<InputValueDefinition> inputValueDefinitions = inputType.getInputValueDefinitions();
            List<Type> inputValueTypes = inputValueDefinitions.stream()
                    .map(InputValueDefinition::getType)
                    .collect(Collectors.toList());

            inputValueTypes.forEach(checkTypeExists("input value", typeRegistry, errors, inputType));

        });
    }

    private void checkDirectiveDefinitions(TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors) {

        List<DirectiveDefinition> directiveDefinitions = new ArrayList<>(typeRegistry.getDirectiveDefinitions().values());

        directiveDefinitions.forEach(directiveDefinition -> {
            List<InputValueDefinition> arguments = directiveDefinition.getInputValueDefinitions();

            checkNamedUniqueness(errors, arguments, InputValueDefinition::getName,
                    (name, arg) -> new NonUniqueNameError(directiveDefinition, arg));

            List<Type> inputValueTypes = arguments.stream()
                    .map(InputValueDefinition::getType)
                    .collect(Collectors.toList());

            inputValueTypes.forEach(
                    checkTypeExists(typeRegistry, errors, "directive definition", directiveDefinition, directiveDefinition.getName())
            );

            directiveDefinition.getDirectiveLocations().forEach(directiveLocation -> {
                String locationName = directiveLocation.getName();
                try {
                    Introspection.DirectiveLocation.valueOf(locationName);
                } catch (IllegalArgumentException e) {
                    errors.add(new DirectiveIllegalLocationError(directiveDefinition, locationName));
                }
            });
        });
    }

    private void checkScalarImplementationsArePresent(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry, RuntimeWiring wiring) {
        typeRegistry.scalars().forEach((scalarName, scalarTypeDefinition) -> {
            WiringFactory wiringFactory = wiring.getWiringFactory();
            ScalarWiringEnvironment environment = new ScalarWiringEnvironment(typeRegistry, scalarTypeDefinition, Collections.emptyList());
            if (!wiringFactory.providesScalar(environment) && !wiring.getScalars().containsKey(scalarName)) {
                errors.add(new MissingScalarImplementationError(scalarName));
            }
        });
    }

    private void checkFieldsAreSensible(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry) {
        Map<String, TypeDefinition> typesMap = typeRegistry.types();

        // objects
        List<ObjectTypeDefinition> objectTypes = filterTo(typesMap, ObjectTypeDefinition.class);
        objectTypes.forEach(objectType -> checkObjTypeFields(errors, objectType, objectType.getFieldDefinitions()));

        // interfaces
        List<InterfaceTypeDefinition> interfaceTypes = filterTo(typesMap, InterfaceTypeDefinition.class);
        interfaceTypes.forEach(interfaceType -> checkInterfaceFields(errors, interfaceType, interfaceType.getFieldDefinitions()));

        // enum types
        List<EnumTypeDefinition> enumTypes = filterTo(typesMap, EnumTypeDefinition.class);
        enumTypes.forEach(enumType -> checkEnumValues(errors, enumType, enumType.getEnumValueDefinitions()));

        // input types
        List<InputObjectTypeDefinition> inputTypes = filterTo(typesMap, InputObjectTypeDefinition.class);
        inputTypes.forEach(inputType -> checkInputValues(errors, inputType, inputType.getInputValueDefinitions()));
    }


    private void checkObjTypeFields(List<GraphQLError> errors, ObjectTypeDefinition typeDefinition, List<FieldDefinition> fieldDefinitions) {
        // field unique ness
        checkNamedUniqueness(errors, fieldDefinitions, FieldDefinition::getName,
                (name, fieldDef) -> new NonUniqueNameError(typeDefinition, fieldDef));

        // field arg unique ness
        fieldDefinitions.forEach(fld -> checkNamedUniqueness(errors, fld.getInputValueDefinitions(), InputValueDefinition::getName,
                (name, inputValueDefinition) -> new NonUniqueArgumentError(typeDefinition, fld, name)));

        // directive checks
        fieldDefinitions.forEach(fld -> checkNamedUniqueness(errors, fld.getDirectives(), Directive::getName,
                (directiveName, directive) -> new NonUniqueDirectiveError(typeDefinition, fld, directiveName)));

        fieldDefinitions.forEach(fld -> fld.getDirectives().forEach(directive -> {
            checkDeprecatedDirective(errors, directive,
                    () -> new InvalidDeprecationDirectiveError(typeDefinition, fld));

            checkNamedUniqueness(errors, directive.getArguments(), Argument::getName,
                    (argumentName, argument) -> new NonUniqueArgumentError(typeDefinition, fld, argumentName));

        }));
    }

    private void checkInterfaceFields(List<GraphQLError> errors, InterfaceTypeDefinition interfaceType, List<FieldDefinition> fieldDefinitions) {
        // field unique ness
        checkNamedUniqueness(errors, fieldDefinitions, FieldDefinition::getName,
                (name, fieldDef) -> new NonUniqueNameError(interfaceType, fieldDef));

        // field arg unique ness
        fieldDefinitions.forEach(fld -> checkNamedUniqueness(errors, fld.getInputValueDefinitions(), InputValueDefinition::getName,
                (name, inputValueDefinition) -> new NonUniqueArgumentError(interfaceType, fld, name)));

        // directive checks
        fieldDefinitions.forEach(fld -> checkNamedUniqueness(errors, fld.getDirectives(), Directive::getName,
                (directiveName, directive) -> new NonUniqueDirectiveError(interfaceType, fld, directiveName)));

        fieldDefinitions.forEach(fld -> fld.getDirectives().forEach(directive -> {
            checkDeprecatedDirective(errors, directive,
                    () -> new InvalidDeprecationDirectiveError(interfaceType, fld));

            checkNamedUniqueness(errors, directive.getArguments(), Argument::getName,
                    (argumentName, argument) -> new NonUniqueArgumentError(interfaceType, fld, argumentName));

        }));
    }

    private void checkEnumValues(List<GraphQLError> errors, EnumTypeDefinition enumType, List<EnumValueDefinition> enumValueDefinitions) {

        // enum unique ness
        checkNamedUniqueness(errors, enumValueDefinitions, EnumValueDefinition::getName,
                (name, inputValueDefinition) -> new NonUniqueNameError(enumType, inputValueDefinition));


        // directive checks
        enumValueDefinitions.forEach(enumValue -> {
            BiFunction<String, Directive, NonUniqueDirectiveError> errorFunction = (directiveName, directive) -> new NonUniqueDirectiveError(enumType, enumValue, directiveName);
            checkNamedUniqueness(errors, enumValue.getDirectives(), Directive::getName, errorFunction);
        });

        enumValueDefinitions.forEach(enumValue -> enumValue.getDirectives().forEach(directive -> {
            checkDeprecatedDirective(errors, directive,
                    () -> new InvalidDeprecationDirectiveError(enumType, enumValue));

            BiFunction<String, Argument, NonUniqueArgumentError> errorFunction = (argumentName, argument) -> new NonUniqueArgumentError(enumType, enumValue, argumentName);
            checkNamedUniqueness(errors, directive.getArguments(), Argument::getName, errorFunction);

        }));
    }

    private void checkInputValues(List<GraphQLError> errors, InputObjectTypeDefinition inputType, List<InputValueDefinition> inputValueDefinitions) {

        // field unique ness
        checkNamedUniqueness(errors, inputValueDefinitions, InputValueDefinition::getName,
                (name, inputValueDefinition) -> {
                    // not sure why this is needed but inlining breaks it
                    @SuppressWarnings("UnnecessaryLocalVariable")
                    InputObjectTypeDefinition as = inputType;
                    return new NonUniqueNameError(as, inputValueDefinition);
                });


        // directive checks
        inputValueDefinitions.forEach(inputValueDef -> checkNamedUniqueness(errors, inputValueDef.getDirectives(), Directive::getName,
                (directiveName, directive) -> new NonUniqueDirectiveError(inputType, inputValueDef, directiveName)));

        inputValueDefinitions.forEach(inputValueDef -> inputValueDef.getDirectives().forEach(directive -> {
            checkDeprecatedDirective(errors, directive,
                    () -> new InvalidDeprecationDirectiveError(inputType, inputValueDef));

            checkNamedUniqueness(errors, directive.getArguments(), Argument::getName,
                    (argumentName, argument) -> new NonUniqueArgumentError(inputType, inputValueDef, argumentName));
        }));
    }


    /**
     * A special check for the magic @deprecated directive
     *
     * @param errors        the list of errors
     * @param directive     the directive to check
     * @param errorSupplier the error supplier function
     */
    static void checkDeprecatedDirective(List<GraphQLError> errors, Directive directive, Supplier<InvalidDeprecationDirectiveError> errorSupplier) {
        if ("deprecated".equals(directive.getName())) {
            // it can have zero args
            List<Argument> arguments = directive.getArguments();
            if (arguments.size() == 0) {
                return;
            }
            // but if has more than it must have 1 called "reason" of type StringValue
            if (arguments.size() == 1) {
                Argument arg = arguments.get(0);
                if ("reason".equals(arg.getName()) && arg.getValue() instanceof StringValue) {
                    return;
                }
            }
            // not valid
            errors.add(errorSupplier.get());
        }
    }

    /**
     * A simple function that takes a list of things, asks for their names and checks that the
     * names are unique within that list.  If not it calls the error handler function
     *
     * @param errors            the error list
     * @param listOfNamedThings the list of named things
     * @param namer             the function naming a thing
     * @param errorFunction     the function producing an error
     */
    static <T, E extends GraphQLError> void checkNamedUniqueness(List<GraphQLError> errors, List<T> listOfNamedThings, Function<T, String> namer, BiFunction<String, T, E> errorFunction) {
        Set<String> names = new LinkedHashSet<>();
        listOfNamedThings.forEach(thing -> {
            String name = namer.apply(thing);
            if (names.contains(name)) {
                errors.add(errorFunction.apply(name, thing));
            } else {
                names.add(name);
            }
        });
    }

    private void checkTypeResolversArePresent(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry, RuntimeWiring wiring) {

        Predicate<InterfaceTypeDefinition> noDynamicResolverForInterface = interaceTypeDef -> !wiring.getWiringFactory().providesTypeResolver(new InterfaceWiringEnvironment(typeRegistry, interaceTypeDef));
        Predicate<UnionTypeDefinition> noDynamicResolverForUnion = unionTypeDef -> !wiring.getWiringFactory().providesTypeResolver(new UnionWiringEnvironment(typeRegistry, unionTypeDef));

        Predicate<TypeDefinition> noTypeResolver = typeDefinition -> !wiring.getTypeResolvers().containsKey(typeDefinition.getName());
        Consumer<TypeDefinition> addError = typeDefinition -> errors.add(new MissingTypeResolverError(typeDefinition));

        typeRegistry.types().values().stream()
                .filter(typeDef -> typeDef instanceof InterfaceTypeDefinition)
                .map(InterfaceTypeDefinition.class::cast)
                .filter(noDynamicResolverForInterface)
                .filter(noTypeResolver)
                .forEach(addError);

        typeRegistry.types().values().stream()
                .filter(typeDef -> typeDef instanceof UnionTypeDefinition)
                .map(UnionTypeDefinition.class::cast)
                .filter(noDynamicResolverForUnion)
                .filter(noTypeResolver)
                .forEach(addError);

    }

    private void checkFieldTypesPresent(TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors, TypeDefinition typeDefinition, List<FieldDefinition> fields) {
        List<Type> fieldTypes = fields.stream().map(FieldDefinition::getType).collect(Collectors.toList());
        fieldTypes.forEach(checkTypeExists("field", typeRegistry, errors, typeDefinition));

        List<Type> fieldInputValues = fields.stream()
                .map(f -> f.getInputValueDefinitions()
                        .stream()
                        .map(InputValueDefinition::getType)
                        .collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        fieldInputValues.forEach(checkTypeExists("field input", typeRegistry, errors, typeDefinition));
    }


    private Consumer<Type> checkTypeExists(String typeOfType, TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors, TypeDefinition typeDefinition) {
        return t -> {
            TypeName unwrapped = TypeInfo.typeInfo(t).getTypeName();
            if (!typeRegistry.hasType(unwrapped)) {
                errors.add(new MissingTypeError(typeOfType, typeDefinition, unwrapped));
            }
        };
    }

    private Consumer<Type> checkTypeExists(TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors, String typeOfType, Node element, String elementName) {
        return ivType -> {
            TypeName unwrapped = TypeInfo.typeInfo(ivType).getTypeName();
            if (!typeRegistry.hasType(unwrapped)) {
                errors.add(new MissingTypeError(typeOfType, element, elementName, unwrapped));
            }
        };
    }

    private Consumer<? super Type> checkInterfaceTypeExists(TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors, TypeDefinition typeDefinition) {
        return t -> {
            TypeInfo typeInfo = TypeInfo.typeInfo(t);
            TypeName unwrapped = typeInfo.getTypeName();
            Optional<TypeDefinition> type = typeRegistry.getType(unwrapped);
            if (!type.isPresent()) {
                errors.add(new MissingInterfaceTypeError("interface", typeDefinition, unwrapped));
            } else if (!(type.get() instanceof InterfaceTypeDefinition)) {
                errors.add(new MissingInterfaceTypeError("interface", typeDefinition, unwrapped));
            }
        };
    }

    private void checkInterfacesAreImplemented(List<GraphQLError> errors, TypeDefinitionRegistry typeRegistry) {
        Map<String, TypeDefinition> typesMap = typeRegistry.types();

        // objects
        List<ObjectTypeDefinition> objectTypes = filterTo(typesMap, ObjectTypeDefinition.class);
        objectTypes.forEach(objectType -> {
            List<Type> implementsTypes = objectType.getImplements();
            implementsTypes.forEach(checkInterfaceIsImplemented("object", typeRegistry, errors, objectType));
        });

        Map<String, List<ObjectTypeExtensionDefinition>> typeExtensions = typeRegistry.objectTypeExtensions();
        typeExtensions.values().forEach(extList -> extList.forEach(typeExtension -> {
            List<Type> implementsTypes = typeExtension.getImplements();
            implementsTypes.forEach(checkInterfaceIsImplemented("extension", typeRegistry, errors, typeExtension));
        }));
    }

    private Consumer<? super Type> checkInterfaceIsImplemented(String typeOfType, TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors, ObjectTypeDefinition objectTypeDef) {
        return t -> {
            TypeInfo typeInfo = TypeInfo.typeInfo(t);
            TypeName unwrapped = typeInfo.getTypeName();
            Optional<TypeDefinition> type = typeRegistry.getType(unwrapped);
            // previous checks handle the missing case and wrong type case
            if (type.isPresent() && type.get() instanceof InterfaceTypeDefinition) {
                InterfaceTypeDefinition interfaceTypeDef = (InterfaceTypeDefinition) type.get();

                Map<String, FieldDefinition> objectFields = objectTypeDef.getFieldDefinitions().stream()
                        .collect(Collectors.toMap(
                                FieldDefinition::getName, Function.identity(), mergeFirstValue()
                        ));

                interfaceTypeDef.getFieldDefinitions().forEach(interfaceFieldDef -> {
                    FieldDefinition objectFieldDef = objectFields.get(interfaceFieldDef.getName());
                    if (objectFieldDef == null) {
                        errors.add(new MissingInterfaceFieldError(typeOfType, objectTypeDef, interfaceTypeDef, interfaceFieldDef));
                    } else {
                        if (!typeRegistry.isSubTypeOf(objectFieldDef.getType(), interfaceFieldDef.getType())) {
                            String interfaceFieldType = AstPrinter.printAst(interfaceFieldDef.getType());
                            String objectFieldType = AstPrinter.printAst(objectFieldDef.getType());
                            errors.add(new InterfaceFieldRedefinitionError(typeOfType, objectTypeDef, interfaceTypeDef, objectFieldDef, objectFieldType, interfaceFieldType));
                        }

                        // look at arguments
                        List<InputValueDefinition> objectArgs = objectFieldDef.getInputValueDefinitions();
                        List<InputValueDefinition> interfaceArgs = interfaceFieldDef.getInputValueDefinitions();
                        if (objectArgs.size() != interfaceArgs.size()) {
                            errors.add(new MissingInterfaceFieldArgumentsError(typeOfType, objectTypeDef, interfaceTypeDef, objectFieldDef));
                        } else {
                            checkArgumentConsistency(typeOfType, objectTypeDef, interfaceTypeDef, objectFieldDef, interfaceFieldDef, errors);
                        }
                    }
                });
            }
        };
    }


    private void checkArgumentConsistency(String typeOfType, ObjectTypeDefinition objectTypeDef, InterfaceTypeDefinition interfaceTypeDef, FieldDefinition objectFieldDef, FieldDefinition interfaceFieldDef, List<GraphQLError> errors) {
        List<InputValueDefinition> objectArgs = objectFieldDef.getInputValueDefinitions();
        List<InputValueDefinition> interfaceArgs = interfaceFieldDef.getInputValueDefinitions();
        for (int i = 0; i < interfaceArgs.size(); i++) {
            InputValueDefinition interfaceArg = interfaceArgs.get(i);
            InputValueDefinition objectArg = objectArgs.get(i);
            String interfaceArgStr = AstPrinter.printAst(interfaceArg);
            String objectArgStr = AstPrinter.printAst(objectArg);
            if (!interfaceArgStr.equals(objectArgStr)) {
                errors.add(new InterfaceFieldArgumentRedefinitionError(typeOfType, objectTypeDef, interfaceTypeDef, objectFieldDef, objectArgStr, interfaceArgStr));
            }
        }
    }

    private Consumer<OperationTypeDefinition> checkOperationTypesExist(TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors) {
        return op -> {
            TypeName unwrapped = TypeInfo.typeInfo(op.getTypeName()).getTypeName();
            if (!typeRegistry.hasType(unwrapped)) {
                errors.add(new MissingTypeError("operation", op, op.getName(), unwrapped));
            }
        };
    }

    private Consumer<OperationTypeDefinition> checkOperationTypesAreObjects(TypeDefinitionRegistry typeRegistry, List<GraphQLError> errors) {
        return op -> {
            // make sure it is defined as a ObjectTypeDef
            Type queryType = op.getTypeName();
            Optional<TypeDefinition> type = typeRegistry.getType(queryType);
            type.ifPresent(typeDef -> {
                if (!(typeDef instanceof ObjectTypeDefinition)) {
                    errors.add(new OperationTypesMustBeObjects(op));
                }
            });
        };
    }

    private <T extends TypeDefinition> List<T> filterTo(Map<String, TypeDefinition> types, Class<? extends T> clazz) {
        return types.values().stream()
                .filter(t -> clazz.equals(t.getClass()))
                .map(clazz::cast)
                .collect(Collectors.toList());
    }

    private <T> BinaryOperator<T> mergeFirstValue() {
        return (v1, v2) -> v1;
    }
}
