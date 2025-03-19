# Elasticsearch docs

> [!IMPORTANT]  
> Elastic docs migration from asciidoc to Markdown is ongoing for 9.0.
Elasticians can reach out in `#docs` or `#es-docs` on Slack if you have questions.

> [!TIP]
> If you need to update the `8.x` docs, you'll have to use the old `asciidoc` system. Refer to the [`8.x` README](https://github.com/elastic/elasticsearch/blob/8.x/docs/README.asciidoc) for more information.

This is a guide for writing, editing, and building the Elasticsearch docs.

## Where do the docs source files live?

As of 9.0.0, the Elasticsearch docs have been redistributed as part of the [Elastic docs migration](https://elastic.github.io/docs-builder/migration/), with the aim of telling a more cohesive story across Elastic products, features, and tools.

Docs live in **three places**:

1. **Reference content** lives in this repo. This covers low-level stuff like settings and configuration information that is tightly coupled to code.
- 👩🏽‍💻 **Engineers** own the bulk of this content.
2.  **API reference docs** live in the [Elasticsearch specification](https://github.com/elastic/elasticsearch-specification/blob/main/README.md#how-to-generate-the-openapi-representation)
    - This is where you need to update API docs published in the [new API docs system](https://www.elastic.co/docs/api/doc/elasticsearch/v8/)
- 👩🏽‍💻 **Engineers** own this content.
3. **Narrative, overview, and conceptual content** mostly lives in the [`docs-content`](https://github.com/elastic/docs-content/) repo. 
- ✍🏼 **Tech writers** own the bulk of this content.

### Where can I find the source files for a specific page?

Once the docs are published, you'll be able to choose the `Edit this page` option to find the source file in the appropriate GitHub repo.

In the meantime, grep around in the Elasticsearch or `docs-content` repos to identify the source file.

## Build the docs

For 9.0.0+, all (ex-API reference) docs are written in [Elastic docs V3 Markdown syntax](https://elastic.github.io/docs-builder/syntax/) and stored in `.md` files.

You can:
* Build the [docs locally](https://elastic.github.io/docs-builder/contribute/locally/)
* Make single-page edits using the [GitHub UI](https://elastic.github.io/docs-builder/contribute/on-the-web/)

### Preview links for PRs

For open PRs, a GitHub action generates a live preview of any docs changes. If the check runs successfully, you can find the preview at:

`https://docs-v3-preview.elastic.dev/elastic/elasticsearch/pull/<PR-NUMBER>/reference/`

To re-run CI checks, an Elastic employee can select the `Re-run this job` option in the GitHub Actions tab. 

## Backporting

> [!TIP]
As of 9.0.0, we are currently only publishing from the `main` branch.
We will continue to backport changes as usual, in case we need to revisit this approach in the future.

If you need to update the `8.x` docs, you'll have to use the old `asciidoc` system. Refer to the [`8.x` README](https://github.com/elastic/elasticsearch/blob/8.x/docs/README.asciidoc) for more information.

> [!NOTE]
> If you need to make changes to 9.x docs and 8.x docs, you'll need to use two different workflows:

- **For `9.x` docs**, create a PR using the new Markdown system against the `main` branch and backport as necessary.
- **For `8.x` docs**, create a PR using the old AsciiDoc system against the `8.x` branch and backport the changes to any other `8.x` branches needed.

## Test code snippets

> [!IMPORTANT]
> Snippet testing has been temporarily disabled for Elasticsearch docs. This is a WIP and should be re-enabled soon for the new Markdown docs.

Snippets in `console` syntax are automatically tested by the command `./gradlew -pdocs check`. To test just the docs from a single page, use e.g. `./gradlew -pdocs yamlRestTest --tests "*rollover*"`.

By default each `console` snippet runs as its own isolated test. You can manipulate the test execution in the following ways:

* `% TEST`: Explicitly marks a snippet as a test. Snippets marked this way are tests even if they don't have `` ```console `` but usually `% TEST` is used for its modifiers:
  * `% TEST[s/foo/bar/]`: Replace `foo` with `bar` in the generated test. This should be used sparingly because it makes the snippet "lie". Sometimes, though, you can use it to make the snippet more clear. Keep in mind that if there are multiple substitutions then they are applied in the order that they are defined.
  * `% TEST[catch:foo]`: Used to expect errors in the requests. Replace `foo` with `request` to expect a 400 error, for example. If the snippet contains multiple requests then only the last request will expect the error.
  * `% TEST[continued]`: Continue the test started in the last snippet. Between tests the nodes are cleaned: indexes are removed, etc. This prevents that from happening between snippets because the two snippets are a single test. This is most useful when you have text and snippets that work together to tell the story of some use case because it merges the snippets (and thus the use case) into one big test.
      * You can't use `% TEST[continued]` immediately after `% TESTSETUP` or `// TEARDOWN`.
  * `% TEST[skip:reason]`: Skip this test. Replace `reason` with the actual reason to skip the test. Snippets without `% TEST` or `// CONSOLE` aren't considered tests anyway but this is useful for explicitly documenting the reason why the test shouldn't be run.
  * `% TEST[setup:name]`: Run some setup code before running the snippet. This is useful for creating and populating indexes used in the snippet. The `name` is split on `,` and looked up in the `setups` defined in `docs/build.gradle`. See `% TESTSETUP` below for a similar feature.
  * `% TEST[teardown:name]`: Run some teardown code after the snippet. This is useful for performing hidden cleanup, such as deleting index templates. The `name` is split on `,` and looked up in the `teardowns` defined in `docs/build.gradle`. See `% TESTSETUP` below for a similar feature.
  * `% TEST[warning:some warning]`: Expect the response to include a `Warning` header. If the response doesn't include a `Warning` header with the exact text then the test fails. If the response includes `Warning` headers that aren't expected then the test fails.
* `` ```console-result ``: Matches this snippet against the body of the response of the last test. If the response is JSON then order is ignored. If you add `% TEST[continued]` to the snippet after `` ```console-result `` it will continue in the same test, allowing you to interleave requests with responses to check.
* `% TESTRESPONSE`: Explicitly marks a snippet as a test response even without `` ```console-result ``. Similarly to `% TEST` this is mostly used for its modifiers.
  * You can't use `` ```console-result `` immediately after `% TESTSETUP`. Instead, consider using `% TEST[continued]` or rearrange your snippets.

  > [!NOTE]  
  > Previously we only used `% TESTRESPONSE` instead of `` ```console-result `` so you'll see that a lot in older branches but we prefer `` ```console-result `` now.

  * `% TESTRESPONSE[s/foo/bar/]`: Substitutions. See `% TEST[s/foo/bar]` for how it works. These are much more common than `% TEST[s/foo/bar]` because they are useful for eliding portions of the response that are not pertinent to the documentation.
    * One interesting difference here is that you often want to match against the response from Elasticsearch. To do that you can reference the "body" of the response like this: `% TESTRESPONSE[s/"took": 25/"took": $body.took/]`. Note the `$body` string. This says "I don't expect that 25 number in the response, just match against what is in the response." Instead of writing the path into the response after `$body` you can write `$_path` which "figures out" the path. This is especially useful for making sweeping assertions like "I made up all the numbers in this example, don't compare them" which looks like `% TESTRESPONSE[s/\d+/$body.$_path/]`.
  * `% TESTRESPONSE[non_json]`: Add substitutions for testing responses in a format other than JSON. Use this after all other substitutions so it doesn't make other substitutions difficult.
  * `% TESTRESPONSE[skip:reason]`: Skip the assertions specified by this response.
* `% TESTSETUP`: Marks this snippet as the "setup" for all other snippets in this file. In order to enhance clarity and simplify understanding for readers, a straightforward approach involves marking the first snippet in the documentation file with the `% TESTSETUP` marker. By doing so, it clearly indicates that this particular snippet serves as the setup or preparation step for all subsequent snippets in the file. This helps in explaining the necessary steps that need to be executed before running the examples. Unlike the alternative convention `% TEST[setup:name]`, which relies on a setup defined in a separate file, this convention brings the setup directly into the documentation file, making it more self-contained and reducing ambiguity. By adopting this convention, users can easily identify and follow the correct sequence of steps to ensure that the examples provided in the documentation work as intended.
* `// TEARDOWN`: Ends and cleans up a test series started with `% TESTSETUP` or `% TEST[setup:name]`. You can use `// TEARDOWN` to set up multiple tests in the same file.
* `// NOTCONSOLE`: Marks this snippet as neither `// CONSOLE` nor `% TESTRESPONSE`, excluding it from the list of unconverted snippets. We should only use this for snippets that *are* JSON but are *not* responses or requests.

In addition to the standard CONSOLE syntax these snippets can contain blocks of yaml surrounded by markers like this:

```
startyaml
  - compare_analyzers: {index: thai_example, first: thai, second: rebuilt_thai}
endyaml
```

This allows slightly more expressive testing of the snippets. Since that syntax is not supported by `` ```console `` the usual way to incorporate it is with a `% TEST[s//]` marker like this:

```
% TEST[s/\n$/\nstartyaml\n  - compare_analyzers: {index: thai_example, first: thai, second: rebuilt_thai}\nendyaml\n/]
```

Any place you can use json you can use elements like `$body.path.to.thing` which is replaced on the fly with the contents of the thing at `path.to.thing` in the last response.
