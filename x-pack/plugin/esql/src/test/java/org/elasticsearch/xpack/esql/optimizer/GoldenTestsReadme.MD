# Golden tests

## What are golden tests?

Golden tests, or [characterization tests](https://en.wikipedia.org/wiki/Characterization_test), are
tests which record (or snapshot) the current behavior of a system, and then verify the behavior has
not changed.
Contrast with regular (unit) tests, which include a specific assertion of some computed value.

## How do I add a new golden test?

`extend` `org.elasticsearch.xpack.esql.optimizer.GoldenTestCase`, and add new tests by invoking the
`runGoldenTest` method.

```java
class MyGoldenTest extends GoldenTestCase {
  public void testBasicFrom() {
    runGoldenTest("FROM employees");
  }
}
```

The test runner will pick up the "orphan" test and create the required outputs (see next section)
based on the test name.
You can also specify which plans you want to verify (all plans are output by default):

```java
public void testBasic() throws Exception {
  runGoldenTest("FROM employees", EnumSet.of(Stage.ANALYZER, Stage.LOGICAL));
}
```

Golden test outputs are stored in a resource test directory depending on the concrete test class and
the test method name, for example,
`/path/golden_tests/my_golden_test/test_basic_from/physical.expected`.
By default, all test methods create a single directory per method, but you can also have further
directory nesting.

```java
public void testNested() throws Exception {
  for (String type : List.of("int", "long", "double")) {
    runGoldenTest(
        "FROM employees | eval emp_no = emp_no :: " + type,
        EnumSet.allOf(Stage.class),
        type); // Creates a nested directory per type.
  }
}
```

You can also use a fluent builder to specify the test options if you wish:

```java
public void testFluent() throws Exception {
  for (String type : List.of("int", "long", "double")) {
    builder("FROM employees | eval emp_no = emp_no :: " + type)
        .nestedPath(type)
        .searchStats(getSearchStats())
        .run();
  }
}
```

## What happens when a golden test fails?

When a golden test fails, the test runner will create a new `.actual` file, in the same directory as
the `.expected` file. `.actual` files are git ignored by default, you can manually delete them after
inspection, or if you wish to avoid the `.actual` file creation, you can use `-Dgolden.noactual`.
If you want to automatically delete them on test success, you can use `-Dgolden.cleanactual`.

## How do I overwrite an existing output?

Of course, sometimes you *want* the output to change.
Use `-Dgolden.overwrite` to **overwrite** (or overwrite) all the existing outputs.
You can use this in combination with `-Dtests.method` to overwrite specific tests.
This can also be used in combination with `git diff` to inspect the differences if you prefer that
method to comparing `.actual` and `.expected` files.

### Which transport version do written files reflect?

Golden tests normally run with a *random* minimum transport version (to cover mixed- and
remote-cluster planning), but written files must not depend on what the randomizer happened to
draw — otherwise regenerating twice can produce different bytes. Writing runs therefore use a
deterministic anchor: a declared version range (next section) is written at the version that
starts it, everything else at the current version, pinned tests at the pin. After writing, the
runner plans once more at the far end of the versions the files cover and fails the write if the
output differs — the plan forks inside that window, and the fork must be declared (see below);
regenerating cannot paper over it.

## Different expectations at different transport versions

Some plans legitimately change shape at a specific transport version. Declare those points and the
runner keeps one set of expected files per version range:

```java
public void testMultiTypeField() {
  builder("FROM employees | WHERE emp_no::keyword == \"10001\"")
      .expectationChangesAt("compact_multi_type_es_field") // the plan changes shape here
      .run();
}
```

- `expectationChangesAt("tv_name")` **splits** coverage: both shapes stay verified. The current
  shape's files stay directly in the test directory, exactly like an unlabeled test; each older
  range's files live in a subdirectory named by the version that ends the range —
  `testMultiTypeField/analysis.expected` (current),
  `testMultiTypeField/before_compact_multi_type_es_field/analysis.expected` (older).
- `since("tv_name")` **removes** coverage below the version. Use it on a new feature's tests with
  the feature's own transport version — plans below it never existed. The two words exist so a
  reviewer sees which of the two happened.

Values are registered transport version names (`TransportVersion.fromName`); declare labels
oldest-first; neither combines with a pinned `transportVersion(...)`. Tests without labels keep
the flat layout, and directory names carry no order, so a new label never renames existing
directories or breaks existing mutes. If one gate changes most of a suite, declare the label once
in the suite's shared builder helper — the failing tests tell you exactly which ones fork.

### Two runs per suite

Suites opt in with:

```java
@ParametersFactory(argumentFormatting = "%1$s")
public static Iterable<Object[]> parameters() {
    return goldenModes();
}

public MyGoldenTests(@Name("mode") String mode) {
    super(mode);
}
```

`testFoo {current}` plans at the current version against the newest range — deterministic, so a
version-gated change fails in the PR that introduces it. `testFoo {historical}` plans once per
range, at a random released version inside it. The two names mute independently: muting a flaky
`{historical}` leaves the `{current}` alarm armed.

### Failure cheat-sheet

- `{current}` red, `{historical}` green: your change is version-gated — add a label with your
  gate's version and regenerate. Skipping the label doesn't work: old ranges regenerate at their
  own starting version, so the old shape comes back and `{current}` stays red.
- `{historical}` flaky on an unlabeled test: you found an undeclared fork; the failure diff shows
  both shapes. Declare it (label or `since`) and regenerate.
- Warning about a dead range: the compatibility floor (advances at major releases) passed a label.
  Harmless, but clean up as the message says — delete the `before_<label>` directory and remove
  the declaration; nothing else moves. `-Dgolden.gc.strict` escalates the warning to a failure,
  for scheduled cleanup sweeps.
- Error that a declaration no longer resolves: the transport version name was retired, meaning the
  code path it gated is gone. Remove the declaration; for a label, also delete its
  `before_<label>` directory (its files described planning that no longer exists).

## How do I run **all** the golden tests?
```sh
./gradlew :x-pack:plugin:esql:test --tests "*GoldenTests"
```
Note that the `GoldenTests` suffix is enforced via a gradle precommit check.

## Drawbacks of golden tests

Golden tests are by their very nature much noisier than regular tests:
They compare the entire output of the system, rather than a specific value.
For example, you might change the name of a local variable in the logical plan, only to change it
back later in the physical plan.
This change has no effect on the final outcome, nor does it incur any runtime cost, but it will
cause the golden test to fail nonetheless if you are verifying that the logical plan hasn't changed.

One should feel comfortable bulldozing failed tests if one expects changes in the underlying tree.
Golden tests should be the *first* line of defense, not the only one!

If you need to have more targeted tests, for example if you want to only verify certain properties
of the plan, or ones that aren't actually present in its string representation, consider adding
regular unit tests instead. Take a look at some of the tests in `LogicalPlanOptimizerTests` or
`PhysicalPlanOptimizerTests` for examples.
