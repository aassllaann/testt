//
// Created by AbsoluDe on 2023/4/25.
//

#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, Field field, Value value) 
    : table_(table), field_(field), value_(value)
{

}
