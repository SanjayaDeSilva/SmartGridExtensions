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

import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;

public class AverageAggregator extends AttributeAggregator {
    private AverageAggregator averageAgg;
    Logger log = Logger.getLogger(AverageAggregator.class);

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Average aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }

        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();

        switch (type) {

            case DOUBLE:
                averageAgg = new AverageAggregator.AverageAggregatorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Average not supported for " + type);
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return averageAgg.getReturnType();
    }

    @Override
    public Object processAdd(Object data) {
        return averageAgg.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data) {
        return new IllegalStateException("Average cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data) {
        return averageAgg.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data) {
        return new IllegalStateException("Average cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset() {
        return averageAgg.reset();
    }

    @Override
    public Object[] currentState() {
        return averageAgg.currentState();
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void restoreState(Object[] state) {
        averageAgg.restoreState(state);
    }

    private class AverageAggregatorDouble extends AverageAggregator {
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
                double average = getAverage(arr);

                return average;
            }
            return 0.0;
        }

        private double getAverage(ArrayList<Double> test) {
            ArrayList<Double> ary = new ArrayList<Double>();

            double j=0.0;
            int count=0;

            for(int i =0; i<test.size(); i++){
                ++count;
                ary.add(test.get(i));
               // System.out.println(ary);
                j+=ary.get(i);
              //  System.out.println(j);
                if(count==5) {
                    j=j/5;

                }

            }

            return j;
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

