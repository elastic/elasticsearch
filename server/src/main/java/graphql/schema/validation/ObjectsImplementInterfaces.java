package graphql.schema.validation;

import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLUnionType;

import java.util.List;
import java.util.Objects;

import static graphql.schema.GraphQLTypeUtil.simplePrint;
import static graphql.schema.GraphQLTypeUtil.isList;
import static graphql.schema.GraphQLTypeUtil.isNonNull;
import static graphql.schema.GraphQLTypeUtil.unwrapOne;
import static graphql.schema.validation.SchemaValidationErrorType.ObjectDoesNotImplementItsInterfaces;
import static java.lang.String.format;

/**
 * Schema validation rule ensuring object types have all the fields that they need to implement the interfaces
 * they say they implement
 */
public class ObjectsImplementInterfaces implements SchemaValidationRule {

    @Override
    public void check(GraphQLFieldDefinition fieldDef, SchemaValidationErrorCollector validationErrorCollector) {
    }

    @Override
    public void check(GraphQLType type, SchemaValidationErrorCollector validationErrorCollector) {
        if (type instanceof GraphQLObjectType) {
            check((GraphQLObjectType) type, validationErrorCollector);
        }
    }

    private void check(GraphQLObjectType objectType, SchemaValidationErrorCollector validationErrorCollector) {
        List<GraphQLOutputType> interfaces = objectType.getInterfaces();
        interfaces.forEach(interfaceType -> {
            // we have resolved the interfaces at this point and hence the cast is ok
            checkObjectImplementsInterface(objectType, (GraphQLInterfaceType) interfaceType, validationErrorCollector);
        });

    }

    // this deliberately has open field visibility here since its validating the schema
    // when completely open
    private void checkObjectImplementsInterface(GraphQLObjectType objectType, GraphQLInterfaceType interfaceType, SchemaValidationErrorCollector validationErrorCollector) {
        List<GraphQLFieldDefinition> fieldDefinitions = interfaceType.getFieldDefinitions();
        for (GraphQLFieldDefinition interfaceFieldDef : fieldDefinitions) {
            GraphQLFieldDefinition objectFieldDef = objectType.getFieldDefinition(interfaceFieldDef.getName());
            if (objectFieldDef == null) {
                validationErrorCollector.addError(
                        error(format("object type '%s' does not implement interface '%s' because field '%s' is missing",
                                objectType.getName(), interfaceType.getName(), interfaceFieldDef.getName())));
            } else {
                checkFieldTypeCompatibility(objectType, interfaceType, validationErrorCollector, interfaceFieldDef, objectFieldDef);
            }
        }
    }

    private void checkFieldTypeCompatibility(GraphQLObjectType objectType, GraphQLInterfaceType interfaceType, SchemaValidationErrorCollector validationErrorCollector, GraphQLFieldDefinition interfaceFieldDef, GraphQLFieldDefinition objectFieldDef) {
        String interfaceFieldDefStr = simplePrint(interfaceFieldDef.getType());
        String objectFieldDefStr = simplePrint(objectFieldDef.getType());

        if (!isCompatible(interfaceFieldDef.getType(), objectFieldDef.getType())) {
            validationErrorCollector.addError(
                    error(format("object type '%s' does not implement interface '%s' because field '%s' is defined as '%s' type and not as '%s' type",
                            objectType.getName(), interfaceType.getName(), interfaceFieldDef.getName(), objectFieldDefStr, interfaceFieldDefStr)));
        } else {
            checkFieldArgumentEquivalence(objectType, interfaceType, validationErrorCollector, interfaceFieldDef, objectFieldDef);
        }
    }

    private void checkFieldArgumentEquivalence(GraphQLObjectType objectTyoe, GraphQLInterfaceType interfaceType, SchemaValidationErrorCollector validationErrorCollector, GraphQLFieldDefinition interfaceFieldDef, GraphQLFieldDefinition objectFieldDef) {
        List<GraphQLArgument> interfaceArgs = interfaceFieldDef.getArguments();
        List<GraphQLArgument> objectArgs = objectFieldDef.getArguments();
        if (interfaceArgs.size() != objectArgs.size()) {
            validationErrorCollector.addError(
                    error(format("object type '%s' does not implement interface '%s' because field '%s' has a different number of arguments",
                            objectTyoe.getName(), interfaceType.getName(), interfaceFieldDef.getName())));
        } else {
            for (int i = 0; i < interfaceArgs.size(); i++) {
                GraphQLArgument interfaceArg = interfaceArgs.get(i);
                GraphQLArgument objectArg = objectArgs.get(i);

                String interfaceArgStr = makeArgStr(interfaceArg);
                String objectArgStr = makeArgStr(objectArg);

                boolean same = true;
                if (!interfaceArgStr.equals(objectArgStr)) {
                    same = false;
                }
                if (!Objects.equals(interfaceArg.getDefaultValue(), objectArg.getDefaultValue())) {
                    same = false;
                }
                if (!same) {
                    validationErrorCollector.addError(
                            error(format("object type '%s' does not implement interface '%s' because field '%s' argument '%s' is defined differently",
                                    objectTyoe.getName(), interfaceType.getName(), interfaceFieldDef.getName(), interfaceArg.getName())));
                }

            }
        }
    }

    private String makeArgStr(GraphQLArgument argument) {
        // we don't do default value checking because toString of getDefaultValue is not guaranteed to be stable
        return argument.getName() +
                ":" +
                simplePrint(argument.getType());

    }

    private SchemaValidationError error(String msg) {
        return new SchemaValidationError(ObjectDoesNotImplementItsInterfaces, msg);
    }

    /**
     * @return {@code true} if the specified objectType satisfies the constraintType.
     */
    boolean isCompatible(GraphQLOutputType constraintType, GraphQLOutputType objectType) {
        if (isSameType(constraintType, objectType)) {
            return true;
        } else if (constraintType instanceof GraphQLUnionType) {
            return objectIsMemberOfUnion((GraphQLUnionType) constraintType, objectType);
        } else if (constraintType instanceof GraphQLInterfaceType && objectType instanceof GraphQLObjectType) {
            return objectImplementsInterface((GraphQLInterfaceType) constraintType, (GraphQLObjectType) objectType);
        } else if (isList(constraintType) && isList(objectType)) {
            GraphQLOutputType wrappedConstraintType = (GraphQLOutputType) unwrapOne(constraintType);
            GraphQLOutputType wrappedObjectType = (GraphQLOutputType) unwrapOne(objectType);
            return isCompatible(wrappedConstraintType, wrappedObjectType);
        } else if (isNonNull(objectType)) {
            GraphQLOutputType nullableConstraint;
            if (isNonNull(constraintType)) {
                nullableConstraint = (GraphQLOutputType) unwrapOne(constraintType);
            } else {
                nullableConstraint = constraintType;
            }
            GraphQLOutputType nullableObjectType = (GraphQLOutputType) unwrapOne(objectType);
            return isCompatible(nullableConstraint, nullableObjectType);
        } else {
            return false;
        }
    }

    boolean isSameType(GraphQLOutputType a, GraphQLOutputType b) {
        String aDefString = simplePrint(a);
        String bDefString = simplePrint(b);
        return aDefString.equals(bDefString);
    }

    boolean objectImplementsInterface(GraphQLInterfaceType interfaceType, GraphQLObjectType objectType) {
        return objectType.getInterfaces().contains(interfaceType);
    }

    boolean objectIsMemberOfUnion(GraphQLUnionType unionType, GraphQLOutputType objectType) {
        return unionType.getTypes().contains(objectType);
    }

}
