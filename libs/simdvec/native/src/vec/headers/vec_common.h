
#ifndef VEC_COMMON_H
#define VEC_COMMON_H

#include <stdint.h>
#include <assert.h>
#include <type_traits>
#include <utility>

template <uintptr_t align>
static inline uintptr_t align_downwards(const void* ptr) {
    static_assert(align > 0 && (align & (align - 1)) == 0, "Align must be a power of 2");
    assert(ptr != 0);

    uintptr_t addr = (uintptr_t)ptr;
    // Round down to align-byte boundary
    addr &= -align;
    assert(addr <= (uintptr_t)ptr);
    return addr;
}

template <typename T>
static inline auto dot_scalar(const T a, const T b) {
    return a * b;
}

template <typename T>
static inline auto sqr_scalar(const T a, const T b) {
    auto d = a - b;
    return d * d;
}

// --- Mapper functions ---
// Each mapper resolves the i-th document vector to a direct pointer.
// All share the same signature so they can be passed as a template
// parameter to the bulk scoring templates (call_i8_bulk, call_f32_bulk, etc.).

// Contiguous layout: vectors are packed sequentially, each `pitch` elements apart.
template <typename T>
static inline const T* sequential_mapper(const T* data, const int32_t i, const int32_t* offsets, const int32_t pitch) {
    return data + (int64_t)i * pitch;
}

// Offset layout: vectors share a common base but are at non-sequential positions
// given by offsets[i], each `pitch` elements from the base.
template <typename T>
static inline const T* offsets_mapper(const T* data, const int32_t i, const int32_t* offsets, const int32_t pitch) {
    return data + (int64_t)offsets[i] * pitch;
}

// Populates out[0..N-1] by invoking mapper for indices [base..base+N-1],
// with a bounds check that sets out-of-range entries to nullptr.
// Uses recursive template instantiation so each index is a compile-time constant.
template <int N, typename TData, typename T, const T*(*mapper)(const TData*, const int32_t, const int32_t*, const int32_t), int I = 0>
static inline void init_pointers(const T** out, const TData* a, int32_t pitch,
                                 const int32_t* offsets, int32_t base, int32_t count) {
    if constexpr (I < N) {
        out[I] = (base + I < count) ? mapper(a, base + I, offsets, pitch) : nullptr;
        init_pointers<N, TData, T, mapper, I + 1>(out, a, pitch, offsets, base, count);
    }
}

// Compile-time "loop" (analogous to a for-loop that is fully unrolled by the
// compiler). Generate calls to f(0), f(1), ..., f(N-1) where each index is a
// compile-time constant that can be used as a template argument or array subscript that
// the compiler resolves statically.
//
// The "loop" is implemented via compile-time recursion: the compiler recursively instantiates
// this function, effectively unrolling it. The net result is equivalent to writing f(0); f(1); ... f(N-1);
// inline, with no loops, branches, or function-call overhead in the compiled output.
template <int N, typename F, int I = 0>
static inline void apply_indexed(F&& f) {
    // This if is a compile-time branch: apply_indexed is compile-time recursive,
    // meaning the compiler will resolve the constexpr and instantiate the next iteration
    // of the template (apply_indexed<N, F, I + 1>).
    // Everything is handled by the compiler, no runtime branch is emitted.
    if constexpr (I < N) {
        // `std::integral_constant<int, I>` is a lightweight type used to wrap
        // a compile time constant (I). On the caller side, when the lambda parameter
        // is declared `auto I`, the compiler recursively instantiates this function and
        // deduces its type as integral_constant<int, 0>, then <int, 1>, etc.
        // This lets `I` be used wherever a compile-time constant is required.
        f(std::integral_constant<int, I>{});

        // `std::forward` is a C++ idiom related to modern C++ move semantics; it passes `f`
        // to the next recursive call preserving its original value category (lvalue
        // or rvalue). Here it simply avoids an unnecessary copy of the lambda.
        apply_indexed<N, F, I + 1>(std::forward<F>(f));
    }
}

// Performs a binary tree reduction on an array of values.
// Recurses, at compile time, to reduce N values to a single value
// using pairwise reduction whilst not introducing
// unneccessary data dependencies.
template <int N, typename T, T(*reduce)(const T, const T)>
static inline T tree_reduce(const T values[N]) {
    static_assert((N & (N - 1)) == 0, "N must be a power of 2");
    static_assert(N > 1, "There must be at least 2 values to reduce (N > 1)");

    if constexpr (N == 2) {
        return reduce(values[0], values[1]);
    } else {
        return reduce(
            tree_reduce<N/2, T, reduce>(values),
            tree_reduce<N/2, T, reduce>(values + N/2));
    }
}

#endif // VEC_COMMON_H
