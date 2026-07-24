[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "products_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared products = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/skilleaseAcademy/fabric-data-days/refs/heads/main/products.csv"), [Delimiter = ",", Columns = 6, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"product_id", type text}, {"product_name", type text}, {"category", type text}, {"list_price", type text}, {"supplier", type text}, {"active", type logical}}),
  #"Replaced value" = Table.ReplaceValue(#"Changed column type", "USD ", "", Replacer.ReplaceText, {"list_price"}),
  #"Split column by delimiter" = Table.SplitColumn(#"Replaced value", "category", Splitter.SplitTextByDelimiter(" > "), {"category.1", "category.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Split column by delimiter", {{"category.1", "category"}, {"category.2", "sub category"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"list_price", type number}})
in
  #"Changed column type 1";
shared products_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "fa72570c-50f3-47bc-9d68-3eade419f811"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "476bb8d1-eb58-415a-b62c-49a777bdecb9"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "products_sample_df", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
