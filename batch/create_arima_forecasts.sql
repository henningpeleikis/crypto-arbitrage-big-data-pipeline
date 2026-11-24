USE mda2_project;

DROP TABLE IF EXISTS arima_forecasts;

CREATE TABLE arima_forecasts (
  asset            VARCHAR(32)   NOT NULL,
  last_date_used   DATE          NOT NULL,
  forecast_date    DATE          NOT NULL,
  predicted_close  DECIMAL(20,8) NOT NULL,
  model            VARCHAR(32)   NOT NULL,

  horizon_days     INT           NOT NULL DEFAULT 7,
  run_date         DATE          NOT NULL,
  updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (asset, forecast_date),

  INDEX idx_run_date (run_date),
  INDEX idx_asset (asset)
);
