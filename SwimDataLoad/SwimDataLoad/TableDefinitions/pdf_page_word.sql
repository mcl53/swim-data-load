CREATE TABLE isl_raw.pdf_page_word
(
    file_name       VARCHAR(128)    NOT NULL
  , file_type       VARCHAR(32)     NOT NULL
  , page_number     INT             NOT NULL
  , word            CHAR(1)         NOT NULL
  , location_x      DECIMAL(18, 14) NOT NULL
  , location_y      DECIMAL(18, 14) NOT NULL
  , width           DECIMAL(18, 14) NOT NULL
  , height          DECIMAL(18, 14) NOT NULL
  , loaded_datetime TIMESTAMP           NULL
);
