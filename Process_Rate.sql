SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [dbo].[Process_Rate] (
	@Transaction_Key VARCHAR(50),
    @Charge_Id int,
    @Rate_Condition_String varchar(100),
    @Range_Factor_Value FLOAT,
    @Revised_Rate FLOAT
)
AS
BEGIN

    --testing the script changes
    --Well that worked
    
    -- dbo.Process_Rate 'CN-1', 1, 'MSK|20|GP|GEN', 25, 0

    SET NOCOUNT ON;
	Declare @Rate_Condition_Id int;
    DECLARE @Rate_Formula varchar(200);
    Declare @Charge_Basis varchar(20);
    Declare @Charge_Type varchar(20);
    Declare @Cummulate_Rate char(1);
    Declare @From_Range FLOAT;
    Declare @To_Range Float;
    Declare @Parent_Charge_Code Varchar(20);
 


    Declare @Rate FLOAT;
    Declare @Composite_Rate FLOAT;
    Declare @Rate_Total FLOAT;

    set @Rate_Total = 0;
    set @Rate = 0;
    set @From_Range = 0;
    set @To_Range = 0;

    Declare @Transaction_Base_Data_with_Rate TABLE 
    (
        Transaction_Key VARCHAR(50),
        Charge_Id int,
        Rate varchar(100),
        Rate_Audit VARCHAR(100)
    )

    DECLARE Rate_Cursor CURSOR FOR 
    select 
            Rate_Master.Rate_Formula
            ,Rate_Master.Charge_Basis
            ,Charge.Cummulate_Rate
            ,charge.Charge_Type
            ,Rate_Master.From_Range
            ,Rate_Master.To_Range
        from Rate_Master 
            inner join Rate_Condition on Rate_Master.Rate_Condition_Id = Rate_Condition.Rate_Condition_Id
            inner join Charge on Rate_Condition.Charge_Id = Charge.Charge_Id
            inner join
            (
                select
                    MIN(Rate_Condition.Rate_Value_Weight) Best_Rate_Condition 
                from Rate_Condition
                inner join Rate_Master on Rate_Condition.Rate_Condition_Id = Rate_Master.Rate_Condition_Id
                where @Rate_Condition_String like Rate_Condition.Rate_Value_String and Rate_Condition.Charge_Id = @Charge_Id
            ) BRC on Rate_Condition.Rate_Value_Weight = BRC.Best_Rate_Condition 
    Where Rate_Condition.Charge_Id = @Charge_Id
    and Rate_Master.From_Range <= @Range_Factor_Value and Rate_Master.To_Range >= @Range_Factor_Value 


    OPEN Rate_Cursor  
    FETCH NEXT FROM Rate_Cursor INTO @Rate_Formula, @Charge_Basis, @Cummulate_Rate, @Charge_Type, @From_Range, @To_Range
    WHILE @@FETCH_STATUS = 0  
    BEGIN  
        IF @Cummulate_Rate = 'Y'
        BEGIN
            set @Rate = cast (@Rate_Formula as float)
            IF @Charge_Basis = 'RANGE_PER_UNIT'
            BEGIN
                set @Rate = @Rate * @Range_Factor_Value
                set @Rate_Total = @Rate_Total + @Rate
            END
            IF @Charge_Basis = 'FLAT'
            BEGIN
                set @Rate_Total = @Rate_Total + @Rate
            END
        END
        ELSE
        BEGIN
            set @Rate_Total = 0;
            IF @Charge_Basis = 'RANGE_PER_UNIT'
            BEGIN
                set @Rate = cast (@Rate_Formula as float)
                set @Rate_Total = @Rate * @Range_Factor_Value
            END
            IF @Charge_Basis = 'FLAT'
            BEGIN
                set @Rate = cast (@Rate_Formula as float)
                set @Rate_Total = @Rate
            END
        END
        IF @Charge_Type = 'COMPOSITE'
        BEGIN   
            DECLARE Composite_Rate_Cursor CURSOR FOR
            SELECT pc.name, TBR.Rate
            FROM #Transaction_Base_Data_with_Rate TBR
                INNER join CompositeCharge cc on cc.Parent_Charge_Id = TBR.Charge_Id
                INNER JOIN Charge PC ON PC.Charge_Id = TBR.Charge_Id
            WHERE TBR.Transaction_Key = @Transaction_Key
  
            OPEN Composite_Rate_Cursor
            FETCH NEXT FROM Composite_Rate_Cursor into @Parent_Charge_Code, @Composite_Rate
            WHILE @@FETCH_STATUS = 0
            BEGIN
                set @Rate_Formula = REPLACE(@Rate_Formula,'{' + @Parent_Charge_Code + '}', cast (@Composite_Rate as varchar(20)))
                FETCH NEXT FROM Composite_Rate_Cursor  into @Parent_Charge_Code, @Composite_Rate
            END
            CLOSE Composite_Rate_Cursor
            DEALLOCATE Composite_Rate_Cursor
        END
        
        DECLARE @Comp_rate float
        DECLARE @vQuery NVARCHAR(1000)
        SET @vQuery = N'SELECT @Comp_rate = ' + @Rate_Formula
        EXEC SP_EXECUTESQL 
                @Query  = @vQuery
            , @Params = N'@Comp_rate float OUTPUT'
            , @Comp_rate = @Comp_rate OUTPUT
        set @Rate_Total = @Comp_rate
        insert into @Transaction_Base_Data_with_Rate values
	    (@Transaction_Key, @Charge_Id, @Rate_Total ,'')

        FETCH NEXT FROM Rate_Cursor INTO @Rate_Formula, @Charge_Basis, @Cummulate_Rate, @Charge_Type, @From_Range, @To_Range
    END 
    CLOSE Rate_Cursor  
    DEALLOCATE Rate_Cursor     

    select * from @Transaction_Base_Data_with_Rate

END;
GO
