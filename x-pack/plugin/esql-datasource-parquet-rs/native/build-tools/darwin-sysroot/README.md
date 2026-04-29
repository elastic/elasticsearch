The files in this directory a minimal TAPI files that tell the linker that
the "this library exists" without providing any symbols or code.

This works in combination with linker parameters `-undefined -C link-arg=dynamic_lookup`
that let it silently ignore missing symbols.

From https://github.com/apple-oss-distributions/tapi:

> TAPI is a **T**ext-based **A**pplication **P**rogramming **I**nterface. It
> replaces the Mach-O Dynamic Library Stub files in Apple's SDKs to reduce SDK
> size even further.
>
> The text-based dynamic library stub file format (.tbd) is a human readable and
> editable text file.

