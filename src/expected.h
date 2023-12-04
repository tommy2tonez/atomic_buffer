#ifndef __DG_EXPECTED_H__
#define __DG_EXPECTED_H__

#include <optional>
#include <type_traits>
#include "assert.h"

namespace dg{

    template <class ErrType>
    struct unexpected_clue{
        ErrType value;
        explicit unexpected_clue(ErrType value): value(value){}
    };

    //temporary solution, waiting c++23
    template <class _Ty, class ErrType>
    class expected: private std::optional<_Ty>,
                    private std::optional<ErrType>{

        public:

            using ValueBase = std::optional<_Ty>;
            using ErrBase   = std::optional<ErrType>;     
            using Self      = expected;

            expected() = delete;

            template <class ...Args>
            expected(Args&& ...args): ValueBase(std::forward<Args>(args)...){
                assert(ValueBase::has_value()); 
            }

            template <class ErrArg>
            expected(unexpected_clue<ErrArg> err): ErrBase(err.value){
                assert(ErrBase::has_value());
            }

            expected(const Self& other): ValueBase(static_cast<const ValueBase&>(other)),
                                         ErrBase(static_cast<const ErrBase&>(other)){}
            
            expected(Self&& other): ValueBase(static_cast<ValueBase&&>(other)),
                                    ErrBase(static_cast<ErrBase&&>(other)){}

            Self& operator =(const Self& other){
                ValueBase::operator =(static_cast<const ValueBase&>(other));
                ErrBase::operator =(static_cast<const ErrBase&>(other));
                return *this;
            }

            Self& operator =(Self&& other){
                ValueBase::operator =(static_cast<ValueBase&&>(other));
                ErrBase::operator =(static_cast<ErrBase&&>(other));
                return *this;
            }

            using ValueBase::operator bool;
            using ValueBase::has_value;
            using ValueBase::value;
            using ValueBase::operator *;
            using ValueBase::operator ->;

            decltype(auto) error() const &{ 
                return ErrBase::value();
            }

            decltype(auto) error() const &&{
                return ErrBase::value();
            }

            decltype(auto) error() &{
                return ErrBase::value();
            }

            decltype(auto) error() &&{
                return ErrBase::value();
            }

            bool operator ==(const Self& other) const{
                if (has_value()){
                    return static_cast<const ValueBase&>(*this) == other;
                }
                return static_cast<const ErrBase&>(*this) == other;
            }
    };

    template <class T>
    auto unexpected(T&& val){
        unexpected_clue rs(std::forward<T>(val));
        return rs;
    }
};
#endif