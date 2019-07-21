package graphql.language;

import graphql.AssertException;
import graphql.PublicApi;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertTrue;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.joining;

/**
 * This can take graphql language AST and print it out as a string
 */
@SuppressWarnings("UnnecessaryLocalVariable")
@PublicApi
public class AstPrinter {

    private final Map<Class<? extends Node>, NodePrinter<? extends Node>> printers = new LinkedHashMap<>();

    private final boolean compactMode;

    private AstPrinter(boolean compactMode) {
        this.compactMode = compactMode;
        printers.put(Argument.class, argument());
        printers.put(ArrayValue.class, value());
        printers.put(BooleanValue.class, value());
        printers.put(NullValue.class, value());
        printers.put(Directive.class, directive());
        printers.put(DirectiveDefinition.class, directiveDefinition());
        printers.put(DirectiveLocation.class, directiveLocation());
        printers.put(Document.class, document());
        printers.put(EnumTypeDefinition.class, enumTypeDefinition());
        printers.put(EnumTypeExtensionDefinition.class, enumTypeExtensionDefinition());
        printers.put(EnumValue.class, enumValue());
        printers.put(EnumValueDefinition.class, enumValueDefinition());
        printers.put(Field.class, field());
        printers.put(FieldDefinition.class, fieldDefinition());
        printers.put(FloatValue.class, value());
        printers.put(FragmentDefinition.class, fragmentDefinition());
        printers.put(FragmentSpread.class, fragmentSpread());
        printers.put(InlineFragment.class, inlineFragment());
        printers.put(InputObjectTypeDefinition.class, inputObjectTypeDefinition());
        printers.put(InputObjectTypeExtensionDefinition.class, inputObjectTypeExtensionDefinition());
        printers.put(InputValueDefinition.class, inputValueDefinition());
        printers.put(InterfaceTypeDefinition.class, interfaceTypeDefinition());
        printers.put(InterfaceTypeExtensionDefinition.class, interfaceTypeExtensionDefinition());
        printers.put(IntValue.class, value());
        printers.put(ListType.class, type());
        printers.put(NonNullType.class, type());
        printers.put(ObjectField.class, objectField());
        printers.put(ObjectTypeDefinition.class, objectTypeDefinition());
        printers.put(ObjectTypeExtensionDefinition.class, objectTypeExtensionDefinition());
        printers.put(ObjectValue.class, value());
        printers.put(OperationDefinition.class, operationDefinition());
        printers.put(OperationTypeDefinition.class, operationTypeDefinition());
        printers.put(ScalarTypeDefinition.class, scalarTypeDefinition());
        printers.put(ScalarTypeExtensionDefinition.class, scalarTypeExtensionDefinition());
        printers.put(SchemaDefinition.class, schemaDefinition());
        printers.put(SelectionSet.class, selectionSet());
        printers.put(StringValue.class, value());
        printers.put(TypeName.class, type());
        printers.put(UnionTypeDefinition.class, unionTypeDefinition());
        printers.put(UnionTypeExtensionDefinition.class, unionTypeExtensionDefinition());
        printers.put(VariableDefinition.class, variableDefinition());
        printers.put(VariableReference.class, variableReference());
    }

    private NodePrinter<Argument> argument() {
        if (compactMode) {
            return (out, node) -> out.printf("%s:%s", node.getName(), value(node.getValue()));
        }
        return (out, node) -> out.printf("%s: %s", node.getName(), value(node.getValue()));
    }

    private NodePrinter<Document> document() {
        if (compactMode) {
            return (out, node) -> out.printf("%s", join(node.getDefinitions(), " "));
        }
        return (out, node) -> out.printf("%s\n", join(node.getDefinitions(), "\n\n"));
    }

    private NodePrinter<Directive> directive() {
        final String argSep = compactMode ? "," : ", ";
        return (out, node) -> {
            String arguments = wrap("(", join(node.getArguments(), argSep), ")");
            out.printf("@%s%s", node.getName(), arguments);
        };
    }

    private NodePrinter<DirectiveDefinition> directiveDefinition() {
        final String argSep = compactMode ? "," : ", ";
        return (out, node) -> {
            String arguments = wrap("(", join(node.getInputValueDefinitions(), argSep), ")");
            String locations = join(node.getDirectiveLocations(), " | ");
            out.printf("directive @%s%s on %s", node.getName(), arguments, locations);
        };
    }

    private NodePrinter<DirectiveLocation> directiveLocation() {
        return (out, node) -> out.print(node.getName());
    }

    private NodePrinter<EnumTypeDefinition> enumTypeDefinition() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s",
                    spaced(
                            "enum",
                            node.getName(),
                            directives(node.getDirectives()),
                            block(node.getEnumValueDefinitions())
                    ));
        };
    }

    private NodePrinter<EnumValue> enumValue() {
        return (out, node) -> out.printf("%s", node.getName());
    }

    private NodePrinter<EnumValueDefinition> enumValueDefinition() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s",
                    spaced(
                            node.getName(),
                            directives(node.getDirectives())
                    ));
        };
    }

    private NodePrinter<Field> field() {
        final String argSep = compactMode ? "," : ", ";
        final String aliasSuffix = compactMode ? ":" : ": ";
        return (out, node) -> {
            String alias = wrap("", node.getAlias(), aliasSuffix);
            String name = node.getName();
            String arguments = wrap("(", join(node.getArguments(), argSep), ")");
            String directives = directives(node.getDirectives());
            String selectionSet = node(node.getSelectionSet());

            out.printf("%s", spaced(
                    alias + name + arguments,
                    directives,
                    selectionSet
            ));
        };
    }


    private NodePrinter<FieldDefinition> fieldDefinition() {
        final String argSep = compactMode ? "," : ", ";
        return (out, node) -> {
            out.printf("%s", comments(node));
            String args;
            if (hasComments(node.getInputValueDefinitions()) && !compactMode) {
                args = join(node.getInputValueDefinitions(), "\n");
                out.printf("%s", node.getName() +
                        wrap("(\n", args, "\n)") +
                        ": " +
                        spaced(
                                type(node.getType()),
                                directives(node.getDirectives())
                        )
                );
            } else {
                args = join(node.getInputValueDefinitions(), argSep);
                out.printf("%s", node.getName() +
                        wrap("(", args, ")") +
                        ": " +
                        spaced(
                                type(node.getType()),
                                directives(node.getDirectives())
                        )
                );
            }
        };
    }

    private boolean hasComments(List<? extends Node> nodes) {
        return nodes.stream().anyMatch(it -> it.getComments().size() > 0);
    }

    private NodePrinter<FragmentDefinition> fragmentDefinition() {
        return (out, node) -> {
            String name = node.getName();
            String typeCondition = type(node.getTypeCondition());
            String directives = directives(node.getDirectives());
            String selectionSet = node(node.getSelectionSet());

            out.printf("fragment %s on %s ", name, typeCondition);
            out.printf("%s", directives + selectionSet);
        };
    }

    private NodePrinter<FragmentSpread> fragmentSpread() {
        return (out, node) -> {
            String name = node.getName();
            String directives = directives(node.getDirectives());

            out.printf("...%s%s", name, directives);
        };
    }

    private NodePrinter<InlineFragment> inlineFragment() {
        return (out, node) -> {
            TypeName typeName = node.getTypeCondition();
            //Inline fragments may not have a type condition
            String typeCondition = typeName == null ? "" : wrap("on ", type(typeName), "");
            String directives = directives(node.getDirectives());
            String selectionSet = node(node.getSelectionSet());

            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    "...",
                    typeCondition,
                    directives,
                    selectionSet
            ));
        };
    }

    private NodePrinter<InputObjectTypeDefinition> inputObjectTypeDefinition() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    "input",
                    node.getName(),
                    directives(node.getDirectives()),
                    block(node.getInputValueDefinitions())
                    )
            );
        };
    }

    private NodePrinter<InputValueDefinition> inputValueDefinition() {
        String nameTypeSep = compactMode ? ":" : ": ";
        String defaultValueEquals = compactMode ? "=" : "= ";
        return (out, node) -> {
            Value defaultValue = node.getDefaultValue();
            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    node.getName() + nameTypeSep + type(node.getType()),
                    wrap(defaultValueEquals, defaultValue, ""),
                    directives(node.getDirectives())
                    )
            );
        };
    }

    private NodePrinter<InterfaceTypeDefinition> interfaceTypeDefinition() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    "interface",
                    node.getName(),
                    directives(node.getDirectives()),
                    block(node.getFieldDefinitions())
                    )
            );
        };
    }

    private NodePrinter<ObjectField> objectField() {
        String nameValueSep = compactMode ? ":" : " : ";
        return (out, node) -> out.printf("%s%s%s", node.getName(), nameValueSep, value(node.getValue()));
    }


    private NodePrinter<OperationDefinition> operationDefinition() {
        final String argSep = compactMode ? "," : ", ";
        return (out, node) -> {
            String op = node.getOperation().toString().toLowerCase();
            String name = node.getName();
            String varDefinitions = wrap("(", join(nvl(node.getVariableDefinitions()), argSep), ")");
            String directives = directives(node.getDirectives());
            String selectionSet = node(node.getSelectionSet());

            // Anonymous queries with no directives or variable definitions can use
            // the query short form.
            if (isEmpty(name) && isEmpty(directives) && isEmpty(varDefinitions) && op.equals("QUERY")) {
                out.printf("%s", selectionSet);
            } else {
                out.printf("%s", spaced(op, smooshed(name, varDefinitions), directives, selectionSet));
            }
        };
    }

    private NodePrinter<OperationTypeDefinition> operationTypeDefinition() {
        String nameTypeSep = compactMode ? ":" : ": ";
        return (out, node) -> out.printf("%s%s%s", node.getName(), nameTypeSep, type(node.getTypeName()));
    }

    private NodePrinter<ObjectTypeDefinition> objectTypeDefinition() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    "type",
                    node.getName(),
                    wrap("implements ", join(node.getImplements(), " & "), ""),
                    directives(node.getDirectives()),
                    block(node.getFieldDefinitions())
            ));
        };
    }

    private NodePrinter<SelectionSet> selectionSet() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s", block(node.getSelections()));
        };
    }

    private NodePrinter<ScalarTypeDefinition> scalarTypeDefinition() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    "scalar",
                    node.getName(),
                    directives(node.getDirectives())));
        };
    }


    private NodePrinter<SchemaDefinition> schemaDefinition() {
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    "schema",
                    directives(node.getDirectives()),
                    block(node.getOperationTypeDefinitions())

            ));
        };
    }


    private NodePrinter<Type> type() {
        return (out, node) -> out.print(type(node));
    }

    private String type(Type type) {
        if (type instanceof NonNullType) {
            NonNullType inner = (NonNullType) type;
            return wrap("", type(inner.getType()), "!");
        } else if (type instanceof ListType) {
            ListType inner = (ListType) type;
            return wrap("[", type(inner.getType()), "]");
        } else {
            TypeName inner = (TypeName) type;
            return inner.getName();
        }
    }

    private NodePrinter<ObjectTypeExtensionDefinition> objectTypeExtensionDefinition() {
        return (out, node) -> out.printf("extend %s", node(node, ObjectTypeDefinition.class));
    }

    private NodePrinter<EnumTypeExtensionDefinition> enumTypeExtensionDefinition() {
        return (out, node) -> out.printf("extend %s", node(node, EnumTypeDefinition.class));
    }

    private NodePrinter<InterfaceTypeDefinition> interfaceTypeExtensionDefinition() {
        return (out, node) -> out.printf("extend %s", node(node, InterfaceTypeDefinition.class));
    }

    private NodePrinter<UnionTypeExtensionDefinition> unionTypeExtensionDefinition() {
        return (out, node) -> out.printf("extend %s", node(node, UnionTypeDefinition.class));
    }

    private NodePrinter<ScalarTypeExtensionDefinition> scalarTypeExtensionDefinition() {
        return (out, node) -> out.printf("extend %s", node(node, ScalarTypeDefinition.class));
    }

    private NodePrinter<InputObjectTypeExtensionDefinition> inputObjectTypeExtensionDefinition() {
        return (out, node) -> out.printf("extend %s", node(node, InputObjectTypeDefinition.class));
    }

    private NodePrinter<UnionTypeDefinition> unionTypeDefinition() {
        String barSep = compactMode ? "|" : " | ";
        String equals = compactMode ? "=" : "= ";
        return (out, node) -> {
            out.printf("%s", comments(node));
            out.printf("%s", spaced(
                    "union",
                    node.getName(),
                    directives(node.getDirectives()),
                    equals + join(node.getMemberTypes(), barSep)
            ));
        };
    }

    private NodePrinter<VariableDefinition> variableDefinition() {
        String nameTypeSep = compactMode ? ":" : ": ";
        String defaultValueEquals = compactMode ? "=" : " = ";
        return (out, node) -> out.printf("$%s%s%s%s",
                node.getName(),
                nameTypeSep,
                type(node.getType()),
                wrap(defaultValueEquals, node.getDefaultValue(), "")
        );
    }

    private NodePrinter<VariableReference> variableReference() {
        return (out, node) -> out.printf("$%s", node.getName());
    }

    private String node(Node node) {
        return node(node, null);
    }

    private String node(Node node, Class startClass) {
        if (startClass != null) {
            assertTrue(startClass.isInstance(node), "The starting class must be in the inherit tree");
        }
        StringWriter sw = new StringWriter();
        PrintWriter out = new PrintWriter(sw);
        NodePrinter<Node> printer = _findPrinter(node, startClass);
        printer.print(out, node);
        return sw.toString();
    }

    @SuppressWarnings("unchecked")
    private <T extends Node> NodePrinter<T> _findPrinter(Node node) {
        return _findPrinter(node, null);
    }

    private <T extends Node> NodePrinter<T> _findPrinter(Node node, Class startClass) {
        if (node == null) {
            return (out, type) -> {
            };
        }
        Class clazz = startClass != null ? startClass : node.getClass();
        while (clazz != Object.class) {
            NodePrinter nodePrinter = printers.get(clazz);
            if (nodePrinter != null) {
                //noinspection unchecked
                return nodePrinter;
            }
            clazz = clazz.getSuperclass();
        }
        throw new AssertException(String.format("We have a missing printer implementation for %s : report a bug!", clazz));
    }

    private <T> boolean isEmpty(List<T> list) {
        return list == null || list.isEmpty();
    }

    private boolean isEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    private <T> List<T> nvl(List<T> list) {
        return list != null ? list : Collections.emptyList();
    }

    private NodePrinter<Value> value() {
        return (out, node) -> out.print(value(node));
    }

    private String value(Value value) {
        String argSep = compactMode ? "," : ", ";
        if (value instanceof IntValue) {
            return valueOf(((IntValue) value).getValue());
        } else if (value instanceof FloatValue) {
            return valueOf(((FloatValue) value).getValue());
        } else if (value instanceof StringValue) {
            return wrap("\"", valueOf(((StringValue) value).getValue()), "\"");
        } else if (value instanceof EnumValue) {
            return valueOf(((EnumValue) value).getName());
        } else if (value instanceof BooleanValue) {
            return valueOf(((BooleanValue) value).isValue());
        } else if (value instanceof NullValue) {
            return "null";
        } else if (value instanceof ArrayValue) {
            return "[" + join(((ArrayValue) value).getValues(), argSep) + "]";
        } else if (value instanceof ObjectValue) {
            return "{" + join(((ObjectValue) value).getObjectFields(), argSep) + "}";
        } else if (value instanceof VariableReference) {
            return "$" + ((VariableReference) value).getName();
        }
        return "";
    }

    private String comments(Node<?> node) {
        List<Comment> comments = nvl(node.getComments());
        if (isEmpty(comments) || compactMode) {
            return "";
        }
        String s = comments.stream().map(c -> "#" + c.getContent()).collect(joining("\n", "", "\n"));
        return s;
    }


    private String directives(List<Directive> directives) {
        return join(nvl(directives), " ");
    }

    private <T extends Node> String join(List<T> nodes, String delim) {
        return join(nodes, delim, "", "");
    }

    @SuppressWarnings("SameParameterValue")
    private <T extends Node> String join(List<T> nodes, String delim, String prefix, String suffix) {
        String s = nvl(nodes).stream().map(this::node).collect(joining(delim, prefix, suffix));
        return s;
    }

    private String spaced(String... args) {
        return join(" ", args);
    }

    private String smooshed(String... args) {
        return join("", args);
    }

    private String join(String delim, String... args) {
        String s = Arrays.stream(args).filter(arg -> !isEmpty(arg)).collect(joining(delim));
        return s;
    }

    String wrap(String start, String maybeString, String end) {
        if (isEmpty(maybeString)) {
            if (start.equals("\"") && end.equals("\"")) {
                return "\"\"";
            }
            return "";
        }
        return start + maybeString + (!isEmpty(end) ? end : "");
    }

    private <T extends Node> String block(List<T> nodes) {
        if (isEmpty(nodes)) {
            return "{}";
        }
        if (compactMode) {
            return "{"
                    + join(nodes, " ")
                    + "}";
        }
        return indent("{\n"
                + join(nodes, "\n"))
                + "\n}";
    }

    private String indent(String maybeString) {
        if (isEmpty(maybeString)) {
            return "";
        }
        maybeString = maybeString.replaceAll("\\n", "\n  ");
        return maybeString;
    }

    @SuppressWarnings("SameParameterValue")
    String wrap(String start, Node maybeNode, String end) {
        if (maybeNode == null) {
            return "";
        }
        return start + node(maybeNode) + (isEmpty(end) ? "" : end);
    }

    /**
     * This will pretty print the AST node in graphql language format
     *
     * @param node the AST node to print
     *
     * @return the printed node in graphql language format
     */
    public static String printAst(Node node) {
        StringWriter sw = new StringWriter();
        printAst(sw, node);
        return sw.toString();
    }

    /**
     * This will pretty print the AST node in graphql language format
     *
     * @param writer the place to put the output
     * @param node   the AST node to print
     */
    public static void printAst(Writer writer, Node node) {
        printImpl(writer, node, false);
    }

    /**
     * This will print the Ast node in graphql language format in a compact manner, with no new lines
     * and comments stripped out of the text.
     *
     * @param node the AST node to print
     *
     * @return the printed node in a compact graphql language format
     */
    public static String printAstCompact(Node node) {
        StringWriter sw = new StringWriter();
        printImpl(sw, node, true);
        return sw.toString();
    }

    private static void printImpl(Writer writer, Node node, boolean compactMode) {
        AstPrinter astPrinter = new AstPrinter(compactMode);
        NodePrinter<Node> printer = astPrinter._findPrinter(node);
        printer.print(new PrintWriter(writer), node);
    }

    /**
     * These print nodes into output writers
     *
     * @param <T> the type of node
     */
    private interface NodePrinter<T extends Node> {
        void print(PrintWriter out, T node);
    }
}
