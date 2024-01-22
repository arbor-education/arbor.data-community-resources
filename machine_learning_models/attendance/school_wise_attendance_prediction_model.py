# This script trains 3 models for each school in your MAT (Random Forest, LGBM and Linear Regression)
# It then outputs the predictions, with the Mean Squared Error.


import snowflake.snowpark as snowpark
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from lightgbm import LGBMRegressor
import pandas as pd
import numpy as np
import statsmodels.api as sm

def main(session: snowpark.Session): 

    # If you are using the custom warehouse you will need to build an intermediate table (ATTENDANCE_MONTHLY_AVERAGE_MAT)
    # Use this SQL to do this: https://github.com/arbor-education/arbor.data-community-resources/tree/main/sql_queries/analysis
    tableName = 'ATTENDANCE_MONTHLY_AVERAGE_MAT'
    sp_df = session.table(tableName)
    pandas_df = sp_df.toPandas()

    pandas_df['DATE'] = pd.to_datetime(pandas_df[['YEAR', 'MONTH']].assign(DAY=1))
    pandas_df.sort_values(['APPLICATION_ID', 'DATE'], inplace=True)
    pandas_df['LAG_1'] = pandas_df.groupby('APPLICATION_ID')['AVG_TRUE_PROPORTION'].shift(1)
    pandas_df.dropna(inplace=True)

    future_predictions = []
    model_mse = []

    for app_id in pandas_df['APPLICATION_ID'].unique():
        app_df = pandas_df[pandas_df['APPLICATION_ID'] == app_id]
        X = app_df[['LAG_1']]
        y = app_df['AVG_TRUE_PROPORTION']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Prepare X for OLS (it requires a constant term for intercept)
        X_train_ols = sm.add_constant(X_train)
        X_test_ols = sm.add_constant(X_test)

        # Train RandomForestRegressor
        rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
        rf_model.fit(X_train, y_train)
        rf_pred_test = rf_model.predict(X_test)
        rf_mse = mean_squared_error(y_test, rf_pred_test)
        model_mse.append({'application_id': app_id, 'model': 'RandomForest', 'mse': rf_mse})

        # Train LGBMRegressor
        lgbm_model = LGBMRegressor(n_estimators=100, random_state=42)
        lgbm_model.fit(X_train, y_train)
        lgbm_pred_test = lgbm_model.predict(X_test)
        lgbm_mse = mean_squared_error(y_test, lgbm_pred_test)
        model_mse.append({'application_id': app_id, 'model': 'LGBM', 'mse': lgbm_mse})

        # Train OLS
        ols_model = sm.OLS(y_train, X_train_ols).fit()
        ols_pred_test = ols_model.predict(X_test_ols)
        ols_mse = mean_squared_error(y_test, ols_pred_test)
        model_mse.append({'application_id': app_id, 'model': 'OLS', 'mse': ols_mse})

        # Last known value for the first prediction
        last_known_value = app_df.iloc[-1]['LAG_1']
        last_date = app_df.iloc[-1]['DATE']
        
        # Predict the next 6 months
        for i in range(1, 7):
            next_month_date = last_date + pd.DateOffset(months=i)
            month_year = next_month_date.strftime('%Y-%m')

            # Make predictions using each model
            rf_pred = rf_model.predict([[last_known_value]])[0]
            lgbm_pred = lgbm_model.predict([[last_known_value]])[0]
            ols_pred = ols_model.predict(sm.add_constant(np.array([[1, last_known_value]])))[0]  # OLS model expects the constant term

            future_predictions.append({
                'application_id': app_id,
                'date': month_year,
                'model': 'RandomForest',
                'prediction': rf_pred,
                'mse': rf_mse
            })
            future_predictions.append({
                'application_id': app_id,
                'date': month_year,
                'model': 'LGBM',
                'prediction': lgbm_pred,
                'mse': lgbm_mse
            })
            future_predictions.append({
                'application_id': app_id,
                'date': month_year,
                'model': 'OLS',
                'prediction': ols_pred,
                'mse': ols_mse
            })

            # Update last_known_value for the next prediction
            last_known_value = (rf_pred + lgbm_pred + ols_pred) / 3  # Averaging predictions for the next step

    # Create a DataFrame with the future predictions
    future_predictions_df = pd.DataFrame(future_predictions)

    # Convert the DataFrame to a Snowpark DataFrame
    snowpark_future_predictions_df = session.createDataFrame(future_predictions_df)

    return snowpark_future_predictions_df
