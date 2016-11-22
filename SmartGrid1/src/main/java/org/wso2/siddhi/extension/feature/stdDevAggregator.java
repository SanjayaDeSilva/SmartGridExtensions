package org.wso2.siddhi.extension.feature;/*
* Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;



public class stdDevAggregator extends AttributeAggregator {
    private stdDevAggregator stdDevAgg;
    Logger log = Logger.getLogger(stdDevAggregator.class);

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("stdDev aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();

        switch (type) {

            case DOUBLE:
                stdDevAgg = new stdDevAggregator.stdDevAggregatorDouble();
                break;
            default:
                throw new OperationNotSupportedException("stdDev not supported for " + type);
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return stdDevAgg.getReturnType();
    }

    @Override
    public Object processAdd(Object data) {
        return stdDevAgg.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data) {
        return new IllegalStateException("stdDev cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data) {
        return stdDevAgg.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data) {
        return new IllegalStateException("stdDev cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset() {
        return stdDevAgg.reset();
    }

    @Override
    public Object[] currentState() {
        return stdDevAgg.currentState();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void restoreState(Object[] state) {
        stdDevAgg.restoreState(state);
    }

    private class stdDevAggregatorDouble extends stdDevAggregator {
        private final Attribute.Type type = Attribute.Type.DOUBLE;
        ArrayList<Double> arr = new ArrayList<Double>();
        private double mean, oldMean, stdDeviation, sum, median;
        private int count = 0;

        @Override
        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {

            count++;
            //System.out.println("process add :"+count);
            arr.add((Double)data);


            if(arr.size()==5){
                double stdDev = getStdDev(arr);

                return stdDev;
            }
            return 0.0;
        }

        private double getStdDev(ArrayList<Double> test) {
            double sum = 0.0;
            double sumsq = 0.0;

            for(double num: test){
                sum += num;
                sumsq += Math.pow(num, 2);
            }

            double mean = sum / 5;
            return Math.sqrt((sumsq - 5*Math.pow(mean, 2)) / 5);
        }

        @Override
        public Object processRemove(Object data) {
            // System.out.println("process remove :"+count);
            arr.remove(0);
            return 0.0;
        }

        @Override
        public Object reset() {

            return 0.0;
        }

        @Override
        public Object[] currentState() {
            return new Object[]{};
        }

        @Override
        public void restoreState(Object[] state) {

        }
    }


}

