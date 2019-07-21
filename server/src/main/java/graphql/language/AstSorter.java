package graphql.language;

import graphql.PublicApi;
import graphql.schema.idl.TypeInfo;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static graphql.util.TreeTransformerUtil.changeNode;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

/**
 * A class that helps you sort AST nodes
 */
@PublicApi
public class AstSorter {

    /**
     * This will sort nodes in specific orders and then alphabetically.
     *
     * The order is :
     * <ul>
     * <li>Query operation definitions</li>
     * <li>Mutation operation definitions</li>
     * <li>Subscriptions operation definitions</li>
     * <li>Fragment definitions</li>
     * <li>Directive definitions</li>
     * <li>Schema definitions</li>
     * <li>Object Type definitions</li>
     * <li>Interface Type definitions</li>
     * <li>Union Type definitions</li>
     * <li>Enum Type definitions</li>
     * <li>Scalar Type definitions</li>
     * <li>Input Object Type definitions</li>
     * </ul>
     *
     * After those groupings they will be sorted alphabetic.  All arguments and directives on elements
     * will be sorted alphabetically by name.
     *
     * @param nodeToBeSorted the node to be sorted
     * @param <T>            of type {@link graphql.language.Node}
     *
     * @return a new sorted node (because {@link graphql.language.Node}s are immutable)
     */
    public <T extends Node> T sort(T nodeToBeSorted) {

        NodeVisitorStub visitor = new NodeVisitorStub() {

            @Override
            public TraversalControl visitDocument(Document node, TraverserContext<Node> context) {
                Document changedNode = node.transform(builder -> {
                    List<Definition> definitions = sort(node.getDefinitions(), comparingDefinitions());
                    builder.definitions(definitions);
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitOperationDefinition(OperationDefinition node, TraverserContext<Node> context) {
                OperationDefinition changedNode = node.transform(builder -> {
                    builder.variableDefinitions(sort(node.getVariableDefinitions(), comparing(VariableDefinition::getName)));
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.selectionSet(sortSelectionSet(node.getSelectionSet()));
                });
                return changeNode(context, changedNode);
            }


            @Override
            public TraversalControl visitField(Field node, TraverserContext<Node> context) {
                Field changedNode = node.transform(builder -> {
                    builder.arguments(sort(node.getArguments(), comparing(Argument::getName)));
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.selectionSet(sortSelectionSet(node.getSelectionSet()));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitFragmentDefinition(FragmentDefinition node, TraverserContext<Node> context) {
                FragmentDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.selectionSet(sortSelectionSet(node.getSelectionSet()));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitInlineFragment(InlineFragment node, TraverserContext<Node> context) {
                InlineFragment changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.selectionSet(sortSelectionSet(node.getSelectionSet()));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitFragmentSpread(FragmentSpread node, TraverserContext<Node> context) {
                FragmentSpread changedNode = node.transform(builder -> {
                    List<Directive> directives = sort(node.getDirectives(), comparing(Directive::getName));
                    builder.directives(directives);
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitDirective(Directive node, TraverserContext<Node> context) {
                Directive changedNode = node.transform(builder -> {
                    List<Argument> arguments = sort(node.getArguments(), comparing(Argument::getName));
                    builder.arguments(arguments);
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitObjectValue(ObjectValue node, TraverserContext<Node> context) {
                ObjectValue changedNode = node.transform(builder -> {
                    List<ObjectField> objectFields = sort(node.getObjectFields(), comparing(ObjectField::getName));
                    builder.objectFields(objectFields);
                });
                return changeNode(context, changedNode);
            }

            // SDL classes here

            @Override
            public TraversalControl visitSchemaDefinition(SchemaDefinition node, TraverserContext<Node> context) {
                SchemaDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.operationTypeDefinitions(sort(node.getOperationTypeDefinitions(), comparing(OperationTypeDefinition::getName)));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitEnumTypeDefinition(EnumTypeDefinition node, TraverserContext<Node> context) {
                EnumTypeDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.enumValueDefinitions(sort(node.getEnumValueDefinitions(), comparing(EnumValueDefinition::getName)));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitScalarTypeDefinition(ScalarTypeDefinition node, TraverserContext<Node> context) {
                ScalarTypeDefinition changedNode = node.transform(builder -> {
                    List<Directive> directives = sort(node.getDirectives(), comparing(Directive::getName));
                    builder.directives(directives);
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitInputObjectTypeDefinition(InputObjectTypeDefinition node, TraverserContext<Node> context) {
                InputObjectTypeDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.inputValueDefinitions(sort(node.getInputValueDefinitions(), comparing(InputValueDefinition::getName)));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitObjectTypeDefinition(ObjectTypeDefinition node, TraverserContext<Node> context) {
                ObjectTypeDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.implementz(sort(node.getImplements(), comparingTypes()));
                    builder.fieldDefinitions(sort(node.getFieldDefinitions(), comparing(FieldDefinition::getName)));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitInterfaceTypeDefinition(InterfaceTypeDefinition node, TraverserContext<Node> context) {
                InterfaceTypeDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.definitions(sort(node.getFieldDefinitions(), comparing(FieldDefinition::getName)));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitUnionTypeDefinition(UnionTypeDefinition node, TraverserContext<Node> context) {
                UnionTypeDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.memberTypes(sort(node.getMemberTypes(), comparingTypes()));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitFieldDefinition(FieldDefinition node, TraverserContext<Node> context) {
                FieldDefinition changedNode = node.transform(builder -> {
                    builder.directives(sort(node.getDirectives(), comparing(Directive::getName)));
                    builder.inputValueDefinitions(sort(node.getInputValueDefinitions(), comparing(InputValueDefinition::getName)));
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitInputValueDefinition(InputValueDefinition node, TraverserContext<Node> context) {
                InputValueDefinition changedNode = node.transform(builder -> {
                    List<Directive> directives = sort(node.getDirectives(), comparing(Directive::getName));
                    builder.directives(directives);
                });
                return changeNode(context, changedNode);
            }

            @Override
            public TraversalControl visitDirectiveDefinition(DirectiveDefinition node, TraverserContext<Node> context) {
                DirectiveDefinition changedNode = node.transform(builder -> {
                    builder.inputValueDefinitions(sort(node.getInputValueDefinitions(), comparing(InputValueDefinition::getName)));
                    builder.directiveLocations(sort(node.getDirectiveLocations(), comparing(DirectiveLocation::getName)));
                });
                return changeNode(context, changedNode);
            }
        };

        AstTransformer astTransformer = new AstTransformer();
        Node newDoc = astTransformer.transform(nodeToBeSorted, visitor);
        //noinspection unchecked
        return (T) newDoc;
    }


    private Comparator<Type> comparingTypes() {
        return comparing(type -> TypeInfo.typeInfo(type).getName());
    }

    private Comparator<Selection> comparingSelections() {
        Function<Selection, String> byName = s -> {
            if (s instanceof FragmentSpread) {
                return ((FragmentSpread) s).getName();
            }
            if (s instanceof Field) {
                return ((Field) s).getName();
            }
            if (s instanceof InlineFragment) {
                TypeName typeCondition = ((InlineFragment) s).getTypeCondition();
                return typeCondition == null ? "" : typeCondition.getName();
            }
            return "";
        };
        Function<Selection, Integer> byType = s -> {
            if (s instanceof Field) {
                return 1;
            }
            if (s instanceof FragmentSpread) {
                return 2;
            }
            if (s instanceof InlineFragment) {
                return 3;
            }
            return 4;
        };
        return comparing(byType).thenComparing(comparing(byName));
    }

    private Comparator<Definition> comparingDefinitions() {
        Function<Definition, String> byName = d -> {
            if (d instanceof OperationDefinition) {
                String name = ((OperationDefinition) d).getName();
                return name == null ? "" : name;
            }
            if (d instanceof FragmentDefinition) {
                return ((FragmentDefinition) d).getName();
            }
            if (d instanceof DirectiveDefinition) {
                return ((DirectiveDefinition) d).getName();
            }
            if (d instanceof TypeDefinition) {
                return ((TypeDefinition) d).getName();
            }
            return "";
        };
        Function<Definition, Integer> byType = d -> {
            if (d instanceof OperationDefinition) {
                OperationDefinition.Operation operation = ((OperationDefinition) d).getOperation();
                if (OperationDefinition.Operation.QUERY == operation || operation == null) {
                    return 101;
                }
                if (OperationDefinition.Operation.MUTATION == operation) {
                    return 102;
                }
                if (OperationDefinition.Operation.SUBSCRIPTION == operation) {
                    return 104;
                }
                return 100;
            }
            if (d instanceof FragmentDefinition) {
                return 200;
            }
            // SDL
            if (d instanceof DirectiveDefinition) {
                return 300;
            }
            if (d instanceof SchemaDefinition) {
                return 400;
            }
            if (d instanceof TypeDefinition) {
                if (d instanceof ObjectTypeDefinition) {
                    return 501;
                }
                if (d instanceof InterfaceTypeDefinition) {
                    return 502;
                }
                if (d instanceof UnionTypeDefinition) {
                    return 503;
                }
                if (d instanceof EnumTypeDefinition) {
                    return 504;
                }
                if (d instanceof ScalarTypeDefinition) {
                    return 505;
                }
                if (d instanceof InputObjectTypeDefinition) {
                    return 506;
                }
                return 500;
            }
            return -1;
        };
        return comparing(byType).thenComparing(byName);
    }

    private SelectionSet sortSelectionSet(SelectionSet selectionSet) {
        if (selectionSet == null) {
            return null;
        }
        List<Selection> selections = sort(selectionSet.getSelections(), comparingSelections());
        return selectionSet.transform(builder -> builder.selections(selections));
    }

    private <T> List<T> sort(List<T> items, Comparator<T> comparing) {
        items = new ArrayList<>(items);
        items.sort(comparing);
        return items;
    }

    private <T, U extends Comparable<? super U>> Comparator<T> comparing(
            Function<? super T, ? extends U> keyExtractor) {
        return Comparator.comparing(keyExtractor, nullsLast(naturalOrder()));
    }

}
