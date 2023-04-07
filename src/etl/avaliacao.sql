-- Databricks notebook source
WITH
tb_pedido AS (
  SELECT
    DISTINCT t1.idPedido,
    t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE
    dtPedido BETWEEN '2017-07-01' AND '2018-01-01'
    AND idVendedor IS NOT NULL
),

tb_join AS (
  SELECT 
    t1.*,
    t2.vlNota

  FROM tb_pedido AS t1

  LEFT JOIN silver.olist.avaliacao_pedido AS t2
  ON t1.idPedido = t2.idPedido
),

tb_summary AS(
  SELECT 
    idVendedor,
    AVG(vlNota) AS avgNota,
    PERCENTILE(vlNota, 0.5) AS medianNota,
    MIN(vlNota) AS minNota,
    MAX(vlNota) AS maxNota,
    COUNT(vlNota) / COUNT(idPedido) AS pctAvaliacao
  FROM tb_join
  GROUP BY idVendedor
)  

SELECT
  '2018-01-01' AS dtReference,
  *
FROM tb_summary
